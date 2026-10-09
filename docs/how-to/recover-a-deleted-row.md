# Recovering a deleted row

This guide shows you how to recover deleted rows by modeling deletion as a field update instead of a `DELETE` operation. Use this pattern when you need to restore deleted rows and track who removed them.

## Prerequisites

Before you begin, ensure you have:

- An entity schema configured with field history tracking.
- Server authentication enabled, if you need history records to identify the actor who deleted the row.

## 1. Model deletion as a captured field

Define your entity schema with a deletion timestamp field and enable field history tracking. In a schema that uses the [access model](declare-roles-and-permissions.md), give the entity no access rule that admits `delete`, so clients cannot issue hard deletes:

```proto
message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		history: {}
		access: {
			allow: [
				{ read: true requires: "view_projects" },
				{ create: true update: true requires: "edit_projects" }
			]
		}
	};
	string id = 1;
	string name = 2 [(syncplane.v1.field_behavior) = { history: true }];
	// Unix millis, 0 while the project is live.
	int64 deleted_at_ms = 3 [(syncplane.v1.field_behavior) = { history: true }];
}
```

Setting `history: true` on `deleted_at_ms` ensures removals are logged with their actor and timestamp.

No rule admits `delete`, so the server refuses the `DELETE` primitive to every caller that it authorizes, including the in-process API, and the TypeScript client rejects `del` at the call site. This prevents clients from bypassing soft deletion. The rules govern the primitive, not deletion as an outcome. The rows that a custom mutator body writes are not checked against the rules, so place checks inside any mutator that deletes.

In a schema that does not use the access model, leave out `access` and refuse the `DELETE` of the entity in `CanMutate`. See [Reserve an operation for trusted writes](authentication-and-permissions.md#reserve-an-operation-for-trusted-writes).

## 2. Delete a row by updating the field

To delete a row, update its timestamp field:

```ts
await client.db.project.mutate.update(id, (p) => {
	p.deletedAtMs = BigInt(Date.now());
});
```

This mutation is optimistic, undoable, and queued offline if needed. The resulting history record includes `deleted_at_ms` in its mask and records the caller's subject ID if the server [authenticates callers](authentication-and-permissions.md).

## 3. Filter removed rows from reads

Because the row remains in the database, queries match it unless you filter it out.

To exclude removed rows from client queries, filter by predicate:

```ts
const live = await client.db.project.query({
	where: (p) => p.deletedAtMs.eq(0n),
});
```

To enforce filtering on the server so removed rows do not reach certain callers, use a [`ReadFilter`](authentication-and-permissions.md):

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

**Note:** A `ReadFilter` that hides removed rows also hides their history, because history records are served under the source row's filter. To view or restore trash, you must use a caller that the filter admits or a mutator running under `Trusted`.

If removed rows are numerous and you want to prevent them from syncing, exclude them with a [subscription predicate](partial-sync.md):

```ts
subscriptions: [ subscription(ProjectSchema, (p) => p.deletedAtMs.eq(0n)) ];
```

When using a subscription predicate, query the trash using `queryServer`. Perform restores with a custom mutator rather than a client-side `update`, because read-modify-write operations fail on keys that the local store does not hold.

## 4. Restore the row

To restore a deleted row, reset the deletion timestamp field to `0n`:

```ts
await client.db.project.mutate.update(id, (p) => {
	p.deletedAtMs = 0n;
});
```

Because the row was not removed by a `DELETE` primitive, no tombstone exists and no re-bootstrap is needed. Fields untouched by the removal remain intact. To restore individual fields that were changed before deletion, use [field-history restore](restore-a-field.md).

## 5. Permanently erase rows when required

To permanently purge rows for retention policies or erasure requests, perform a `DELETE` using the [`Trusted`](server-side-writes.md) bypass:

```go
err := srv.Mutate(server.TrustedAs(ctx, "retention"), ws, func(tx *server.EntityTx) error {
	return tx.Delete(&appv1.Project{}, id)
})
```

A trusted delete purges the row and its history in the same transaction.

## Verifying the restore

To verify that the row is restored, query for live records:

```ts
const live = await client.db.project.query({
	where: (p) => p.deletedAtMs.eq(0n),
});
```

Confirm that the query returns the restored row and that its previous field values are intact.

## Related

- [Restore a field to a previous value](restore-a-field.md)
- [Proto options → Operations no rule admits](../reference/proto-options.md#operations-no-rule-admits)
- [Limits and trade-offs](../explanation/limits-and-trade-offs.md#field-history-is-a-restore-mechanism-not-an-audit-log)
