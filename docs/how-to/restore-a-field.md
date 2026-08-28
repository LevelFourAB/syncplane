# Restoring a field to a previous value

This guide shows you how to restore an entity field to a previous value using field history. Use this process when you need to recover earlier values for specific fields without overwriting unrelated data.

## Prerequisites

Before you begin, ensure you have:

- A Syncplane server and TypeScript client configured in your project.
- An entity defined with Protocol Buffers (Protobuf).
- Active network connectivity to the server.

## 1. Opt the entity into field history

Enable field history in your Protobuf schema by setting `history: {}` on the entity and marking each field to track with `history: true`:

```proto
message Issue {
	option (syncplane.v1.entity) = {
		name: "issue"
		key_fields: [ "id" ]
		history: {}
	};
	string id = 1;
	string title = 2 [(syncplane.v1.field_behavior) = { history: true }];
	bytes description = 3 [(syncplane.v1.field_behavior) = { text: true history: true }];
	int64 sort_key = 4; // no mark, so reordering never touches the log
}
```

Field capture is explicit: the server records only marked fields. An entity that opts into history without marking at least one field is rejected at registration. Unmarked fields, such as `sort_key`, do not generate history records when modified.

The engine registers the history entity (`syncplane.v1.FieldHistory`, wire name `syncplane_field_history`) when an entity opts in. The server captures changes across all write paths, including client pushes, mutators, connectors, and trusted writes.

For details on record contents, see [Proto options → Field history](../reference/proto-options.md#field-history). For retention and admission-hook options, see [Go server API → Field history](../reference/server.md#field-history).

## 2. Register the history entity on the client

Field history is a `server_only` entity that does not sync automatically. The client reads history records on demand through `queryServer`.

Add `FieldHistorySchema` to your client configuration:

```ts
import { FieldHistorySchema } from 'syncplane/gen/syncplane/v1/history_pb.js';

const client = new SyncClient({
	entities: [ IssueSchema, FieldHistorySchema ],
	// …
});
```

Registering the entity does not sync local rows or require a re-bootstrap. Do not add `FieldHistorySchema` to subscription sets, because subscribing to a server-only entity throws an error.

## 3. Read the history log

To fetch history records for an entity row, query the server:

```ts
const log = await client.queryServer(FieldHistorySchema, {
	where: (h) => and(h.sourceEntity.eq('issue'), h.sourceKey.eq(issueId)),
	orderBy: (h) => [ h.seq.desc() ],
	limit: 50,
});
```

`queryServer` requires active network connectivity. Field history is not cached locally, so history cannot be read offline.

The server checks permissions using the read filter of the source row. A user who cannot read `issue/i-42` receives no history records for that row. You do not need to register a separate filter for the history entity.

Each returned history row provides the following fields:

- `recordedAt`: The timestamp of the change.
- `actorSubject`: The user who made the change, or `actorSystem` if written by the engine or a connector.
- `mask.paths`: The fields that changed during the session.

An entry represents an editing session rather than an individual write. Sequential writes to one row by the same actor fold into a single record. `prior` holds the values before the session began, `seq` through `throughSeq` specifies the sequence range, and `writes` contains the number of folded writes. You can configure the session window using [`WithHistorySession`](../reference/server.md#field-history).

## 4. Restore the field value

To restore a previous value, decode `prior` into the source entity type and pass the recorded mask to `client.mutate.patch`:

```ts
import { fromBinary } from '@bufbuild/protobuf';

const rec = log[0]!.value;
const prior = fromBinary(IssueSchema, rec.prior);
await client.mutate.patch(IssueSchema, issueId, prior, {
	paths: rec.mask!.paths,
});
```

In Go, call `tx.Patch(prior, rec.GetMask().GetPaths(), nil)`. If the entity contains a text field, pass `server.WithTextPeer(...)` to specify the identity under which the restored edits are minted.

Restoring a collaborative text field requires an implementation that resolves the recorded value into edits, such as the TypeScript client or server-side Go. A direct RPC client can restore non-text fields by removing the text field from the mask, leaving text field restoration to a server-side handler. See [Write from server-side code](server-side-writes.md#writing-collaborative-text).

To display a recorded value without restoring it, read text fields with `plainTextOf` (in Go, `text.PlainText`).

## 5. Show before and after values

A history record contains the value a field had before the write. The value it changed to is the next newer recorded value or the live row. To construct a timeline, query records in descending order and fold each record's `prior` value back over a cursor:

```ts
import { mergeMask } from 'syncplane';

const log = await client.queryServer(FieldHistorySchema, logOf(issueId));

// The cursor starts at the row as it is now and steps back one write per entry.
let after = (await client.get(IssueSchema, issueId))!;
const timeline = log.map(({ value: rec }) => {
	const before = fromBinary(IssueSchema, rec.prior);
	const paths = rec.mask?.paths ?? [];
	const entry = {
		seq: rec.seq,
		at: rec.recordedAt,
		by: rec.actorSubject,
		paths,
		before,
		after,
	};
	after = { ...after };
	// The same merge the server applied, so nested and keyed paths land the
	// same way here as they did there.
	mergeMask(IssueSchema, after, before, paths);
	return entry;
});
```

Each entry in `timeline` contains `entry.before` for the previous values and `entry.after` for the resulting values across all fields in `paths`.

Read text values on both sides with `plainTextOf`, which accepts both the recorded format and the live row document.

**Note:** The oldest entry represents row creation only when `rowCreated` is `true`. Otherwise, log retention or page limits truncated earlier history.

## Confirming the restore

To verify that the field was restored:

1. Fetch the entity row using `client.get(IssueSchema, issueId)`.
2. Verify that the field value matches the restored `prior` value.

## Related

- [Limits and trade-offs](../explanation/limits-and-trade-offs.md)
