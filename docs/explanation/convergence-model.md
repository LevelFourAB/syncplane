# The convergence model

This document explains how syncplane converges concurrent state between
clients and the server, why the system uses this design, and what each decision
costs.

## Optimistic overlay and rebase

Mutations apply locally to an **optimistic overlay** and a pending queue. A
background coordinator pushes the queue, pulls server changes, and rebases:

1. Apply the server patch.
2. Drop the mutations that the server acknowledged.
3. Clear the overlay.
4. Replay the remaining pending mutations on the new base.

Reads query the merged view. The user interface reflects local edits
immediately and converges without visual flicker when the server response
arrives.

## One sequence per workspace

The server linearizes mutations per workspace with a monotonic sequence.
Applying `PATCH` field masks in sequence order provides **per-field
last-writer-wins (LWW)** resolution. The system does not store or compare
per-field timestamps.

### "Last writer" means last to reach the server

Conflict resolution order follows the push-arrival sequence, not the wall-clock
time. When a client that was offline for a week reconnects and pushes its queue,
it receives the newest sequence numbers. Its edits overwrite modifications made
by other clients while it was disconnected on a per-field basis.

Field masks limit overwrites to the fields that the client modified. This is an
accepted trade-off for collaborative applications. If you need to detect a
concurrent edit instead of silently merging it, use compare-and-swap (see
[Opting out of silent convergence](#opting-out-of-silent-convergence)) rather
than time ordering. Using a hybrid logical clock (HLC) per mutation compared
per field is the only alternative for real-time ordering, and is out of scope.

### A mask through a nested message does not move its presence

Message presence is observable. You can read presence with `isSet` and
`isNotSet`, or use it in subscription predicates. Because presence is
observable, a mask that names a leaf inside a nested message (such as
`settings.color`) modifies only that leaf:

- The container message appears when a write sets a field inside it.
- The container disappears when a write unsets all fields inside it.
- A patch that is a no-op leaves container presence unchanged on both sides of
  the merge.

The trade-off is that an empty message container does not survive a masked merge.
Writing `settings: {}` and then patching `settings.color` to unset drops
`settings` completely, because the merge logic treats an empty container and an
absent container identically. If you need to preserve container presence,
include an explicit field.

### A oneof is one field

A field mask names the entire **oneof group**, not an individual member (for
example, `choice`, not `choice_text`). The server replaces the whole group: the
writer's active case wins, and a write with an empty group clears the oneof.
Individual members cannot be masked; a mask that names a member is rejected by
both the client and the server.

The group must be the atomic unit. Protocol Buffers allows only one member of a
oneof to hold a value. A member-level mask could specify that `choice_text` is
now `x`, but it cannot define what unsetting `choice_text` means for sibling
fields. Interpreting whether `choice_number` survives or the entire oneof becomes
empty are both possible interpretations, and per-field LWW cannot resolve this
ambiguity. Naming the group eliminates ambiguity and prevents client and server
implementations from drifting.

Writes to a oneof group participate in per-field LWW like any other field. A
concurrent write to an unrelated field (such as `label`) does not affect the
group. When two writers modify the oneof group concurrently, the last write to
arrive wins the entire group rather than combining values.

This design makes the group **atomic**. A message inside a oneof is replaced
as a whole, and mask paths cannot descend into it. For this reason, schema
registration rejects types that require structural merging or permanent
presence inside a oneof, including key fields, keyed-list identities, and
collaborative-text fields (see
[proto options](../reference/proto-options.md#what-a-oneof-cannot-hold)). A
reference may be a oneof member because a stored stub is replaced whole anyway.
A oneof of references declares a
[polymorphic reference](../reference/proto-options.md#polymorphic-references),
where the group is the relation and each member is a case a row can hold.

Reads operate differently because reading individual members carries no
ambiguity. You can address a member directly in queries, orderings, and indexes,
and its presence indicates whether that case is active:

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

You can opt out of automatic merging by passing `{ expectedVersion }` to
`mutate.put`, `mutate.patch`, or `mutate.del`. The server applies the mutation
only if the row's current version matches `expectedVersion`. Otherwise, the
server rejects the mutation with a `conflict` status, rolls back the local
optimistic write, and fires `onMutationRejected`.

You can read the current version from `Row.version` (using `scan`, `query`, or
`watch`) or by calling `client.getRow`. Passing `expectedVersion: 0n` creates a
create-if-absent guard that succeeds only if the row does not exist. The version
equals the workspace sequence number of the write that last modified the row. An
absent or deleted row returns a version of `0`, regardless of tombstone garbage
collection (GC).

Version guards are useful when concurrent edits must be detected rather than
merged. However, consider the following constraints:

- Guards cover the whole row rather than individual fields. A guard fails even
  if a concurrent change touched only unrelated fields.
- `Row.version` advances only when the client pulls from the server. If a
  client issues multiple writes to the same row between pulls, each write names
  the same `expectedVersion`. The first write to reach the server advances the
  row version, causing subsequent writes in that batch to reject against their
  own predecessor. Rapid successive edits on a single row should use plain LWW
  instead of guards.

The client combines un-pushed writes that share a guard into a single guarded
mutation that succeeds or fails as a unit. In React bindings, `useRow` mutation
methods accept `{ cas: true }` to bind the row's current version, but default to
unguarded writes to prevent self-conflict.

## Rejections are results, not errors

Each mutation in a push batch executes inside a database savepoint. If a
mutation fails business validation, the server rolls back that mutation while
committing the remaining mutations in the batch. The server still acknowledges
the rejected mutation (`ack`), causing the client to remove it from the queue
and invoke `onMutationRejected`, but the mutation consumes no sequence number.

Rejections persist through dropped push responses. The server records the
verdict against the client watermark in the transaction that updates the
watermark. If the client resends the push, the server returns `REJECTED` with
the original error code and message instead of returning `ALREADY_APPLIED`.

On the client, a queue with an unacknowledged push retains its mutations across
rebases rather than retiring them based on watermark progress alone. This ensures
that the client resends the push to retrieve the rejected verdict before firing
`onMutationRejected`, which occurs one synchronization cycle later.

The server retains rejection verdicts within the following limits:

- The server drops verdicts oldest-first when stored codes and messages exceed
  64 KiB per client.
- Garbage collection (`WithGC`) clears the verdicts of a client that has not
  pushed within the tombstone retention. The watermark stays, so a later resend
  is answered with a bare `ALREADY_APPLIED` and is still not applied.

## Atomic batches

You can bundle multiple CRUD mutations into a single transaction using
`client.transact(fn)`. The batch executes atomically on the server using one
savepoint, one sequence number, and one acknowledgment or rejection. It forms a
single entry in the client queue and a single undo step.

Within `fn`, subsequent operations observe earlier writes. However, the batch
remains invisible to reads until `fn` completes, and rolls back completely if
`fn` throws an error. Custom mutators cannot run inside a `transact()` batch.

The ledger read, the writes, and the key claim all commit in **one transaction**
under the workspace's write exclusion, so a concurrent retry can never see
"unclaimed" while the first call is mid-write, and no outcome is recorded that
the store did not keep.

That coupling also decides what happens on failure: an error or `Reject` rolls
the key back with the writes, leaving the operation retryable — only committed
outcomes replay.

## Idempotency

Syncplane achieves idempotency through per-client mutation ID watermarks,
checked for contiguity per `(workspace, caller, client_id)`. The caller is the
authenticated principal, by issuer and subject, and every anonymous caller
counts as one caller. In-process writes that support retries use a separate
ledger keyed by a caller-supplied idempotency key, scoped to the caller in the
same way; trusted calls share one key space of their own. Caller-supplied keys
do not require contiguity and survive idle-client garbage collection.

The `client_id` parameter is an unauthenticated handle asserted by the client.
It does not represent identity; do not use it for authorization decisions. The
watermark names the caller for this reason: a principal that pushes under
another principal's `client_id` moves only its own watermark, so it cannot use
up or skip the IDs that the other principal sends.

## Deletes and the horizon

Deleting a row creates a **tombstone** so that offline clients receive the
deletion on their next pull. Garbage collection removes tombstones after a
retention window and advances the workspace horizon. If a client cursor
predates the horizon, the client transparently performs a full re-bootstrap.

Configure the retention window to exceed your longest anticipated client
offline duration. A client that returns after the window re-bootstraps, but its
watermark survives: the pending queue it brings back is resent against the
same idempotency record it was pushed against. Only `WithClientRetention`
deletes watermark records, and a client whose record is gone is re-admitted at
its next pushed mutation ID; see
[Limits and trade-offs](limits-and-trade-offs.md#deleting-idle-client-watermarks-can-apply-a-resend-twice)
for what that costs.

## A bootstrap survives a dropped connection

Snapshot bootstrapping streams in chunks. The client stores staged rows and an
opaque progress token durably in local storage. If the connection drops during
download, the client resumes the snapshot by sending the token to receive only
the remaining chunks. Closing the browser tab does not restart the bootstrap.

The resumed snapshot stream retains the cursor assigned during its initial
attempt, ensuring consistent state across chunk boundaries. Because rows can
span multiple workspace states across pages, the pull that follows the snapshot
starts from that initial cursor to re-deliver changes written since the snapshot
began.

The server validates whether a snapshot can resume. The server requires a fresh
bootstrap and informs the client on `Begin` under any of the following
conditions:

- The subscription set changes.
- The cursor falls behind the garbage collection horizon.
- The target store does not match the store that issued the cursor.
- More access moves come after the cursor than `Limits.MaxResyncScan`, so the
  pull after the snapshot would reset (see
  [the access model](access-model.md)).

When a fresh bootstrap is required, the client discards the partial snapshot.

## Workspaces are a hard partition

The `workspace_id` is a hard partition key everywhere, and the server sequences
per workspace. Each workspace keeps its own cursor, pending queue, and
mutation-id counter. The server requires gap-free IDs per
`(workspace, caller, client_id)`.

Sharing occurs at the transport and storage layers only: a single client can
manage multiple workspaces simultaneously, sharing a local store, change
stream, and push transport while maintaining independent cursors, pending
queues, and mutation ID counters for each workspace.

Writes, `transact()` batches, undo operations, and `queryServer` calls cannot
cross workspace boundaries. Reads can query across workspaces when explicitly
requested. An entity reference resolves in its holder's workspace, unless the
schema declares it as a
[workspace-qualified reference](../reference/proto-options.md#syncplanev1reference--workspace-qualified-references),
whose stored value names the workspace its target lives in; the client resolves
those across the workspaces it holds, the server inside one.

Clients self-assert `workspace_id`. The server enforces access boundaries
through `CanAccessWorkspace`; the default authorization policy allows all
access.

## Schema evolution is additive

`PUT` operations replace entire rows. Stale writers—such as older client builds
or server components deserializing into outdated message definitions—silently
drop unknown fields when overwriting rows. To prevent data loss, use `PATCH`
with field masks for partial updates, and design entity schema changes to be
additive.

A client that reads a row, modifies a single field, and writes back the entire
row overwrites any concurrent changes to other fields. To prevent this, the
TypeScript client's `patch` method rejects full row objects and provides
`update(key, recipe)` instead, masking only the fields modified by the recipe
function (see [Writes](../reference/typescript-client.md#which-write-verb)).

Collaborative-text fields are the exception to whole-row replacement: they union
with the stored document on every `PUT` path.

### A client holds only the entities it declares

Clients and servers maintain separate schema registries that can diverge. When
a server deploys new schemas, a full-sync client receives all server entities,
including entities omitted from its own build. The client cannot decode, index,
or query undeclared schemas, so it drops those rows. Similarly, the server never
returns rows for entities missing from its own catalog.

Registering a previously unsynced entity triggers a workspace re-bootstrap.
Because existing rows were written before the client's current cursor, a normal
pull cannot retrieve them; a snapshot bootstrap is required. This is a one-time
cost paid when a build containing the new schema runs for the first time.

When multiple browser tabs run different builds, they share a single local
store. The store syncs the union of all registered schemas across open tabs. A
tab that lacks an entity schema still pulls rows to keep the store current for
tabs that declare it. The tab includes those entity names in pull requests
because decoding is required to apply collaborative-text deltas (see
[Collaborative text](collaborative-text.md#downstream-pulls-ship-deltas)).

Tabs also share the pending mutation queue. When a tab pulls changes, it rebases
the queue against its local schema registry. If the queue contains mutations for
unknown entities or fields declared only in newer builds, the tab skips
optimistic application, logs an error to `console.error`, and retains the entry
in the queue until push retirement. The confirmed row displays in that tab until
the server acknowledges the write, while tabs with the updated schema display
optimistic updates. Replaying unknown mutations is skipped rather than failing
the rebase to prevent stalling cursor advancement in the shared workspace.

## Related

- [Limits and trade-offs](limits-and-trade-offs.md): Consequences and system
  constraints.
- [Partial sync and coverage](partial-sync-and-coverage.md): Synchronization
  behavior when a client syncs a subset of a workspace.
