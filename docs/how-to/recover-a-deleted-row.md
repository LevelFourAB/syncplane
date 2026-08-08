# Recover a deleted row

Someone removes a project and it has to come back — with a record of who removed
it. Model the removal as a **field** rather than as a `DELETE`, reserve the real
`DELETE` for erasure, and both answers fall out of writes the engine already
records.

## Why not a DELETE

A `DELETE` leaves a tombstone and takes the row's data with it. Nothing can
bring it back: a restore is a PATCH, and a PATCH of a tombstoned row is a
conflict under delete-wins.
[Field history](restore-a-field.md) does not close the gap either — it records
what marked fields held before a write, never the row's final state, and a
`DELETE` purges the row's records in the same transaction. See
[Limits and trade-offs](../explanation/limits-and-trade-offs.md#field-history-is-a-restore-mechanism-not-an-audit-log).

A soft delete keeps the row. Every mechanism that works on live rows — history,
restore, permissions, queries — keeps working on it.

## 1. Model the deletion as a captured field

```proto
message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		delete_policy: DELETE_POLICY_TRUSTED_ONLY
		history: {}
	};
	string id = 1;
	string name = 2 [(syncplane.v1.field_behavior) = { history: true }];
	// Unix millis, 0 while the project is live.
	int64 deleted_at_ms = 3 [(syncplane.v1.field_behavior) = { history: true }];
}
```

`history: true` on the field is what makes the removal show up in the log with
its actor and timestamp.
[`delete_policy: DELETE_POLICY_TRUSTED_ONLY`](../reference/proto-options.md#delete-policy)
refuses the `DELETE` primitive to every caller the server authorizes — including
the in-process API — so a client cannot route around the soft delete. It governs
the primitive, not deletion as an outcome: a custom mutator is authorized by its
invocation, so put the check in the body of any mutator that deletes.

## 2. Delete by writing the field

```ts
await client.db.project.mutate.update(id, (p) => {
	p.deletedAtMs = BigInt(Date.now());
});
```

An ordinary optimistic mutation — offline-queueable, undoable, and captured. The
history record it produces names `deleted_at_ms` in its mask and carries the
actor, which is the "who deleted this" a `DELETE` cannot answer — as long as the
server
[authenticates its callers](authentication-and-permissions.md), since the record
holds the caller's subject and nothing else.

## 3. Keep removed rows out of ordinary reads

The row still exists, so every query still sees it.

Client reads exclude them by predicate:

```ts
const live = await client.db.project.query({
	where: (p) => p.deletedAtMs.eq(0n),
});
```

A [`ReadFilter`](authentication-and-permissions.md) makes the server enforce it,
for when removed rows must not reach some callers at all:

```go
func (p *policy) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	if entity != "project" {
		return nil, nil
	}
	return func(_ string, msg proto.Message) bool {
		return msg.(*appv1.Project).GetDeletedAtMs() == 0
	}, nil
}
```

A `ReadFilter` that hides removed rows also hides their history — a record is
served under the source row's filter — so the trash view and the restore path
both need a caller the filter admits, or a mutator that does the work under
`Trusted`.

Where removed rows are numerous enough to be worth not syncing, exclude them
with a [subscription predicate](partial-sync.md) instead:

```ts
subscriptions: [ subscription(ProjectSchema, (p) => p.deletedAtMs.eq(0n)) ];
```

The trash view is then a `queryServer` read, and the restore goes through a
mutator rather than the step 4 `update` — a read-modify-write of a key the local
store does not hold throws.

## 4. Restore

```ts
await client.db.project.mutate.update(id, (p) => {
	p.deletedAtMs = 0n;
});
```

The row never left, so there is no tombstone to conflict with and no
re-bootstrap. Fields the removal did not touch are still there. To put back a
value an earlier write overwrote, that is the ordinary
[field-history restore](restore-a-field.md), unchanged by any of this.

## 5. Reserve the real DELETE for erasure

Purging for real — a retention job, a subject-erasure request — is a `DELETE`
under the [`Trusted`](server-side-writes.md) bypass, which is the only thing
`DELETE_POLICY_TRUSTED_ONLY` admits:

```go
err := srv.Mutate(server.TrustedAs(ctx, "retention"), ws, func(tx *server.EntityTx) error {
	return tx.Delete(&appv1.Project{}, id)
})
```

That purges the row's history with it, in the same transaction. For an erasure
path that is the requirement rather than a limitation: a log of prior values is
exactly what erasure has to reach.

## What this costs

- **Removed rows still sync.** They are live rows, so they count against
  bootstrap size and partial-sync windows until something erases them. Step 3's
  subscription predicate is the lever; use it before the trash outgrows the
  working set.
- **Every read has to filter.** A query that forgets `deletedAtMs.eq(0n)` shows
  removed rows. A `ReadFilter` makes the server enforce it, at the price of
  hiding the rows from the restore path as well.
- **Erasure needs a job.** Rows that are only ever soft-deleted are never
  collected. Give the trash a retention of its own and run the step 5 delete
  against it.

## Related

- [Restore a field to a previous value](restore-a-field.md) — the field-history
  half.
- [Proto options → Write policies](../reference/proto-options.md#delete-policy).
- [Limits and trade-offs](../explanation/limits-and-trade-offs.md#field-history-is-a-restore-mechanism-not-an-audit-log)
  — why history cannot do this on its own.
