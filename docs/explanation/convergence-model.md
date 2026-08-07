# The convergence model

Everything in syncplane follows from a small set of decisions. This page states
them, and what each one costs.

## Optimistic overlay and rebase

Mutations apply to a local **optimistic overlay** and queue. A background
coordinator pushes the queue, pulls server changes, and **rebases**:

1. apply the server patch,
2. drop the mutations the server acknowledged,
3. clear the overlay,
4. replay the still-pending mutations on the new base.

Reads go through the merged view, so the UI sees local edits immediately and
converges without a flash when the server's answer arrives.

## One sequence per workspace

The server linearizes mutations per workspace with a monotonic sequence.
Applying PATCH field masks in sequence order is **per-field last-writer-wins**.
There are no per-field timestamps anywhere.

### "Last writer" means last to reach the server

Order is the push-arrival sequence, not wall clock. A client offline for a week
pushes on reconnect, gets the _newest_ sequence numbers, and its stale edits win
per-field over everything written while it was gone.

Field masks keep the damage to the fields that client actually touched. This is
an accepted trade-off for Linear-class apps. If you need to _notice_ a
concurrent edit instead of silently merging, reach for compare-and-swap below,
not time-ordering — an HLC per mutation compared per field would be the only
path to real time-order, and is out of scope.

### A mask through a nested message does not move its presence

Message presence is observable — `isSet`/`isNotSet` can read it, and a
subscription predicate can be built on it. So a mask that names a leaf inside a
nested message (`settings.color`) only ever writes that leaf: the container
appears when the write puts something in it, and disappears when the write
leaves it empty. A patch that turns out to be a no-op leaves presence exactly as
it found it, on either side of the merge.

The trade-off is at the other end: a deliberately present-but-empty message does
not survive a masked merge into it. Writing `settings: {}` and then patching
`settings.color` to unset drops `settings` entirely, because an empty container
and an absent one are indistinguishable to the merge. Carry a real field if the
distinction matters.

### A oneof is one field

A mask names the **group**, never a member — `choice`, not `choice_text` — and
the whole group is replaced: the writer's active case wins, and a writer whose
group is empty clears it. Members are not maskable at all; a mask naming one is
rejected on both sides.

The group has to be the unit. Protobuf lets only one member hold a value, so a
member-granular mask can say "`choice_text` is now `x`" but cannot say what
"`choice_text` is unset" means for its siblings — "`choice_number` survives" and
"the oneof is now empty" are both defensible readings of the same mask, and
per-field LWW gives no way to choose. Naming the group asks the question the
data can answer, and the two implementations cannot drift on the answer.

Writes to the group still take part in per-field LWW like any other field: a
concurrent writer who touched only `label` does not disturb it. Two writers who
both touch the group resolve as one field — the last to arrive wins the case,
not a blend of the two.

The cost is that the group is **atomic**: a message inside a oneof is replaced
whole, and no mask path descends into it. That is also why nothing that must be
reliably present may live inside a oneof — key fields, keyed-list identities,
collaborative-text fields and references are all refused at registration (see
[proto options](../reference/proto-options.md#what-a-oneof-cannot-hold)).

Reading is the other way round, because it has no such ambiguity: a **member** is
addressable on its own in queries, orderings and indexes, and its presence is
exactly "this case is active".

```ts
// write: the group
await client.mutate.patch(WidgetSchema, 'w1', {
	choice: { case: 'choiceNumber', value: 42 },
});

// read: the member
const q = { where: (w) => w.choice.choiceText.eq('hit') };
const anyText = { where: (w) => w.choice.choiceText.isSet() };
```

## Opting out of silent convergence

Pass `{ expectedVersion }` to any `mutate.put/patch/del`. The server applies it
only if the row's current version still matches, otherwise it rejects with
`conflict` (rolled back locally, surfaced via `onMutationRejected`).

Read the base version from `Row.version` (`scan` / `query` / `watch`) or
`client.getRow`. `expectedVersion: 0n` means "apply only if the row does not
exist" — create-if-absent. The version is the workspace sequence that last wrote
the row, so an absent or deleted row reads as 0 regardless of tombstone GC.

A guard is worth reaching for when a concurrent edit must be **noticed** rather
than merged, and it is worth knowing what it costs before you do. It covers the
whole row — there are no per-field versions — so it also fires on a change to a
field yours does not touch. And `Row.version` only ever advances on a pull, so
every write a client issues between two pulls names the same version: the first
to reach the server moves the row past it and the rest are rejected against
their own predecessor. A burst of edits on one row therefore wants plain
last-writer-wins, not a guard.

The client folds a burst that shares one guard into a single guarded mutation,
which applies or rejects as a unit — but only while the burst is still
un-pushed, so this narrows the window rather than closing it. In React,
`useRow`'s writers take `{ cas: true }` to bind the row's current version; they
are unguarded otherwise, for the reason above.

## Rejections are results, not errors

Each mutation in a push batch runs inside a **savepoint**: a business rejection
rolls back everything that mutation wrote while the rest of the batch commits. A
rejected mutation is still an **ack** — the client drops it and fires
`onMutationRejected` — but it consumes no sequence number.

A rejection survives a lost push response. The verdict is recorded against the
client's watermark in the same transaction that bumps it, so the resend is
answered `REJECTED` with the original code and message rather than a bare
`ALREADY_APPLIED` that would drop the edit unexplained. On the client, a queue
whose push went unanswered keeps its acked mutations through the next rebase
instead of retiring them on the watermark alone — otherwise the resend that
collects the verdict would never happen. The callback therefore fires one sync
cycle later than it would have.

Two bounds are worth knowing, both reached only by a client that is no longer
really trying: verdicts are dropped oldest-first past 64 KiB of code and message
text per client, and they are pruned with the watermark row itself once
idle-client GC collects it (`WithGC`), after which a resend re-applies through
watermark adoption instead.

## Atomic batches

`client.transact(fn)` bundles the CRUD mutations issued inside `fn` into one
batch: all-or-nothing on the server (one savepoint, one sequence, one
ack-or-rejection), one queue entry, and one undo step.

Sub-mutations apply in order — a later one sees earlier ones — but the batch is
invisible to reads until `fn` returns, and is discarded whole if `fn` throws.
Custom mutators, the other route to multi-row invariants, cannot join a batch.

## Idempotency

Idempotency rides on per-client mutation-id **watermarks**, contiguity-checked
per `(workspace, client_id)`. In-process writes that may be retried use a
separate ledger keyed by a caller-supplied idempotency key, because an API
caller's retry key can satisfy neither contiguity nor survive a client being
pruned as idle.

`client_id` is an unauthenticated idempotency handle the client asserts. It is
**not an identity** — never derive authorization from it.

## Deletes and the horizon

Row deletes leave **tombstones** so offline clients learn about them on the next
pull. GC removes tombstones past a retention window and advances the workspace
horizon; a client whose cursor predates the horizon transparently re-bootstraps.

Keep the retention comfortably above the longest client offline window. A client
pruned as idle that later returns is re-admitted at whatever mutation id it
pushes next.

## A bootstrap survives a dropped connection

A snapshot streams in chunks, and the client keeps what arrives — staged rows
plus an opaque token saying how far the server got. A dropped connection is
therefore not a lost download: the next attempt hands the token back and is
served only the rest. This is durable rather than in-memory, so closing the tab
mid-snapshot does not restart it either.

The resumed stream keeps the cursor its first attempt was given, which is what
makes a snapshot spread over several attempts equivalent to one. Rows are a
mixture of workspace states either way — that is already true across the pages of
a single attempt — and the pull that follows the snapshot runs from that first
cursor, so everything written since is re-delivered no matter which attempt
shipped a given row.

The server decides whether a resume can be honoured. A changed subscription set,
a cursor that has fallen behind the GC horizon, or a store that is not the one
that issued the cursor all mean a fresh snapshot, and the client is told so on
`Begin` — it then discards the partial snapshot instead of merging two.

## Workspaces are a hard partition

`workspace_id` is a hard partition key everywhere, and the server sequences per
workspace. A client can hold several workspaces at once — they share one store,
one change stream and one push — but each keeps its own cursor, pending queue
and mutation-id counter.

A write, a `transact()` batch, an undo step, a reference and `queryServer` never
cross a workspace. Reads may, on request.

`workspace_id` is also **self-asserted** by the client. `CanAccessWorkspace` is
what turns it into a boundary; the default policy admits everyone.

## Schema evolution is additive

PUT replaces whole rows, so a stale writer — an old client binary, or server code
deserializing into an outdated message type — silently drops fields it does not
know about. Prefer PATCH with a field mask for partial updates, and treat entity
schema evolution as additive.

The same shape has a second, everyday form: a client that reads a row, edits one
field of it, and writes the whole thing back carries its own stale copy of every
other field, undoing whatever landed in between. That is why the TS client's
`patch` refuses a whole row as its init and offers `update(key, recipe)` instead
— it masks only the fields the recipe changed. See
[Writes](../reference/typescript-client.md#which-write-verb).

Collaborative-text fields are the one exception to whole-row replacement: they
union with the stored document on every PUT path.

### A client holds only the entities it declares

The two sides keep separate registries, and they routinely disagree: a full-sync
client is sent every entity the **server** has, which after a server deploy may
include ones its own build never declared. Rows it has no schema for cannot be
decoded, indexed or read, so it does not store them — and the server, for the
mirror-image reason, never emits rows for an entity missing from **its** catalog,
whichever sync path asks.

Declaring an entity the store has not synced before therefore re-bootstraps the
workspace. Its rows were all written below the cursor the client has already
advanced past, so no pull can deliver them; a snapshot is the only way in. It is
a one-time cost, paid the first time a build with the new entity starts.

Tabs of different builds share one store, so what a store syncs is the union of
every registry that has opened it, not the registry of whichever tab started
last. A tab that cannot read an entity still keeps its rows current for the tab
that can. It names those entities on every pull, because a row it cannot decode
is also one it cannot fold collaborative-text delta state into — see
[Collaborative text](collaborative-text.md#downstream-pulls-ship-deltas).

The pending queue is shared the same way, and every rebase replays it against the
registry of whichever tab is pulling — so a tab routinely replays a mutation written
against a schema it does not have: an unknown entity, or a PATCH whose mask names
a field only the newer build declares. It skips that entry, reports it over
`console.error`, and leaves it queued for the push that retires it. What is lost
is optimism, not the write: the tab that can apply it holds the optimistic state,
and this one shows the confirmed row until the ack lands. Refusing to replay it
would stall the workspace instead, because the transaction that rebuilds the
overlay is the one advancing the cursor.

## Related

- [Limits and trade-offs](limits-and-trade-offs.md) — the consequences, listed.
- [Partial sync and coverage](partial-sync-and-coverage.md) — what happens when
  a client holds only part of the workspace.
