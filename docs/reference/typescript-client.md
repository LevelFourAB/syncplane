# TypeScript client API reference

The `syncplane` package is a local-first sync client for JavaScript and TypeScript.
Mutations apply optimistically to a local store and queue locally when offline.
A background coordinator pushes the queue, pulls server changes, and rebases local state.
Reads are local and synchronous after an initial await.

- The package is ESM-only (`"type": "module"`).
- Package entry points are `.` (core) and `./react` (React hooks layer; `react` is an optional peer dependency `>=18`).
- Generated Protocol Buffers descriptors are re-exported under `./gen/*`.
- The wire contract is Protocol Buffers.
  Entities are compiled from `.proto` files using `buf` or declared in TypeScript using the `schema` builder.
  Both approaches provide a tuple of `DescMessage` descriptors to the client.

Two surfaces interact with the engine:

1. **Schema-first**: Methods take a `DescMessage` schema as the first argument (`client.get(Todo, key)`).
   Use this surface when two entities share a simple name.
2. **Entity facade (`client.db.<entity>`)**: A typed facade derived from the message simple name (`client.db.todo.get(key)`).
   Throws at build time if two entities map to the same key.

Generic conventions: `D extends DescMessage`, `MessageShape<D>` represents the decoded row object, and `MessageInitShape<D>` represents the partial initialization form accepted by write operations.

## Construction and lifecycle

The following example initializes and closes a client:

```ts
import { SyncClient } from 'syncplane';
import { TodoSchema } from './gen/example/todo/v1/todo_pb.js';

const client = new SyncClient({
	url: 'https://sync.example.com', // or pass a prebuilt `transport`
	workspaceId: 'ws-123',
	entities: [ TodoSchema ], // `const`-inferred tuple -> types `client.db`
});

await client.start(); // bootstrap (first run) + start poke and poll loops
await client.close(); // stop sync, drop watchers, close KV; not reopenable
```

### `new SyncClient(opts: SyncClientOptions<E>)`

The `SyncClientOptions<E extends readonly DescMessage[]>` interface accepts the following options:

| Option                    | Type                                           | Required | Default                                 | Description                                                                                                                                                     |
| ------------------------- | ---------------------------------------------- | -------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `entities`                | `E` (`readonly DescMessage[]`)                 | Required | None                                    | All syncable entity descriptors. Pass as a `const` tuple to type `client.db`.                                                                                   |
| `workspaceId`             | `string`                                       | Optional | None                                    | Shorthand for `workspaces: [id], defaultWorkspace: id`. Supply either `workspaceId` or `workspaces`.                                                            |
| `workspaces`              | `string[]`                                     | Optional | None                                    | Workspaces to sync. Add and remove workspaces at runtime.                                                                                                       |
| `defaultWorkspace`        | `string`                                       | Optional | Single workspace ID                     | Target workspace for client-level APIs (`client.mutate`, `client.db`, `client.get`, `transact`, undo). Required when configuring more than one workspace.       |
| `accountId`               | `string`                                       | Optional | Single workspace ID                     | Storage scope for the IndexedDB database, key-value mutex, change channel, and leadership lock. Required when starting with more than one workspace.            |
| `storagePrefix`           | `string`                                       | Optional | `'sync'`                                | Prefix for names claimed on the origin. Accepts letters, digits, `_`, and `-`, starting with an alphanumeric character. See [Storage names](#storage-names).    |
| `url`                     | `string`                                       | Optional | None                                    | Base URL of the server. Supply either `url` or `transport`.                                                                                                     |
| `transport`               | `Transport`                                    | Optional | None                                    | Prebuilt Connect transport (`@connectrpc/connect`).                                                                                                             |
| `wireFormat`              | `'binary' \| 'json'`                           | Optional | `'binary'`                              | Encoding of the transport built from `url`. Ignored when `transport` is provided.                                                                               |
| `kvStore`                 | `KVStore`                                      | Optional | IndexedDB in browsers, memory elsewhere | Storage backend.                                                                                                                                                |
| `mutators`                | `Record<string, MutatorMirror<DescMessage>>`   | Optional | None                                    | Local mirrors of server-side custom mutators.                                                                                                                   |
| `mergers`                 | `MergerSpec[]`                                 | Optional | None                                    | Client mirrors of server merge hooks for opaque fields. Divergences heal on the next rebase.                                                                    |
| `onMutationRejected`      | `(info: RejectionInfo) => void`                | Optional | None                                    | Callback for rejected mutations or poison drops.                                                                                                                |
| `onStatusChange`          | `(status: SyncStatus) => void`                 | Optional | None                                    | Callback invoked when synchronization status changes.                                                                                                           |
| `onSyncEvent`             | `(event: SyncEvent) => void`                   | Optional | None                                    | Observability handler for push, pull, bootstrap, rebase, poke, stream, leadership, and local query events. See [Telemetry](telemetry.md#the-typescript-client). |
| `pollIntervalMs`          | `number`                                       | Optional | `60000`                                 | Fallback poll interval in milliseconds. Setting `0` disables polling.                                                                                           |
| `pokeEnabled`             | `boolean`                                      | Optional | `true`                                  | Maintains the poke stream.                                                                                                                                      |
| `streamEnabled`           | `boolean`                                      | Optional | `true`                                  | Holds the live change stream open. Setting `false` falls back to poll and poke streams.                                                                         |
| `pushDebounceMs`          | `number`                                       | Optional | `50`                                    | Milliseconds to buffer local writes before pushing. Further writes restart the timer. Setting `0` pushes on every write.                                        |
| `pushMaxDelayMs`          | `number`                                       | Optional | `250`                                   | Maximum wait ceiling in milliseconds from the first buffered write. Setting `0` removes the ceiling.                                                            |
| `pullLimit`               | `number`                                       | Optional | `0`                                     | Maximum row changes per pull page. `0` uses the server default.                                                                                                 |
| `poisonThreshold`         | `number`                                       | Optional | `5`                                     | Identical push failures of the head mutation before dropping to dead-letter storage. `internal` never counts. `<= 0` disables dropping.                         |
| `maxMutationsPerPush`     | `number`                                       | Optional | `256`                                   | Maximum batch size per push RPC.                                                                                                                                |
| `maxWorkspacesPerRequest` | `number`                                       | Optional | `64`                                    | Maximum workspaces named in a single request. Extra workspaces sync by polling.                                                                                 |
| `subscriptions`           | `SubscriptionSpec[]`                           | Optional | `[]`                                    | Partial synchronization scope. Empty or absent values perform full synchronization.                                                                             |
| `holdGraceMs`             | `number`                                       | Optional | `30000`                                 | Milliseconds a row stays in scope after its last hold is released. `0` drops it on the next sync cycle. See [Holds](#holds-one-row-in-scope-on-demand).         |
| `bootstrapPriority`       | `readonly DescMessage[]`                       | Optional | None                                    | Priority order for entities delivered during the first snapshot.                                                                                                |
| `schemaRevision`          | `string`                                       | Optional | None                                    | Opaque token matched against the server schema for skew negotiation.                                                                                            |
| `onSchemaSkew`            | `(info: {client, server}) => SchemaSkewAction` | Optional | `'warn'` when `schemaRevision` is set   | Handler returning `'ignore'`, `'warn'`, `'block'`, or `'resync'`.                                                                                               |
| `undo`                    | `UndoOptions`                                  | Optional | None                                    | Enables in-memory undo and redo stacks.                                                                                                                         |
| `debugVerifyTouches`      | `boolean`                                      | Optional | `SYNC_DEBUG_VERIFY_TOUCHES` value       | Cross-checks maintained watch results against full scans and throws on divergence.                                                                              |
| `debugVerifyIndexReads`   | `boolean`                                      | Optional | `SYNC_DEBUG_VERIFY_TOUCHES` value       | Cross-checks index queries against scans and throws on divergence.                                                                                              |

The store retains only the entities declared in `entities`.
The client discards rows for undeclared schemas received from the server.
Adding an entity to a later build triggers a one-time re-bootstrap of the workspace ([why](../explanation/convergence-model.md#a-client-holds-only-the-entities-it-declares)).

The client invokes callbacks in isolation.
The client logs errors thrown inside callbacks to `console.error` and ignores them.
They do not fail the synchronization cycle and do not interrupt subsequent callbacks.
A throwing `onSchemaSkew` handler defaults to `'warn'`.

### Lifecycle and control methods

The client provides the following lifecycle and control methods:

```ts
client.defaultWorkspace: string | undefined  // readonly; undefined when there is no default
client.workspaceId: string                   // readonly; throws when there is no default

await client.start(): Promise<void>          // idempotent and retryable; begins background sync
await client.close(): Promise<void>          // idempotent; teardown, local data kept; not reopenable
await client.purgeLocalData(): Promise<DiscardedMutation[]>  // delete account data, then close
await client.syncNow(): Promise<void>        // force one push and pull cycle; requires start() first
await client.clientId(): Promise<string>     // stable per-device ID persisted in KV
await client.pendingCount(): Promise<number> // locally queued, unacknowledged mutations

client.status(): SyncStatus                          // snapshot (works before start())
client.onStatusChange(cb): () => void                // subscribe; returns unsubscribe function
client.onMutationRejected(cb): () => void            // multi-listener callback for rejections
client.onSyncEvent(cb): () => void                   // multi-listener callback for sync events
client.setSubscriptions(specs: SubscriptionSpec[]): void  // swap partial sync scope at runtime
client.hold(schema, key): HoldHandle                     // keep one row in scope on demand
```

`client.close()` tears down client resources and is idempotent.
It leaves the store on the device for the next client session on the account.
A closed client throws `"SyncClient is closed"` and cannot be reopened.
Create a new instance instead.

`purgeLocalData()` stops synchronization, deletes all account data (workspace rows, overlay, staging, cursors, queues, and `clientId`), and closes the client.
The client discards queued mutations rather than pushing them, returns them in the result array, and emits them to `onMutationRejected` with code `workspace_purged`.
The IndexedDB database remains in place and empty.
Calling `purgeLocalData()` on a closed client returns an empty array.
See [Wipe local data on sign-out](../how-to/sign-out.md).

### Workspaces

Client-level read, write, transaction, and undo APIs target `defaultWorkspace`.
When no default workspace is configured, these methods throw and operations must go through `client.ws(id)`.
See [Hold several workspaces on one client](../how-to/multiple-workspaces.md).

```ts
client.ws(workspaceId): WorkspaceClient<E>   // throws for a workspace not held
client.addWorkspace(workspaceId, specs?): WorkspaceClient<E>  // bootstraps on the next cycle
await client.removeWorkspace(workspaceId, opts?): Promise<RemoveWorkspaceResult>

interface RemoveWorkspaceOptions {
	purge?: boolean; // also delete local rows, overlay, cursor, and queue
}
interface RemoveWorkspaceResult {
	removed: boolean; // false when the client did not hold it
	purged: boolean;
	discarded: DiscardedMutation[]; // queued mutations dropped by purge
}
```

A `WorkspaceClient` binds reads, `mutate`, `db`, `transact`, `text`, `hold`, `undoScope`, `pendingCount`, `setSubscriptions`, and `status()` to one workspace.
Cross-workspace reads (`scanAll`, `queryAll`, `watchQueryAll`) remain on the root client and label each row with its `workspaceId`.

When `purge` is `true` in `removeWorkspace`, the client deletes the workspace's local rows, overlay, cursors, and mutation queue.
The client returns queued mutations in `discarded` and emits them to `onMutationRejected` with code `workspace_purged`.
Removing the default workspace leaves the client without a default.

## Reads

All local read operations merge the optimistic local overlay over confirmed server state.

```ts
// Schema-first
await client.get(Todo, key):    Promise<MessageShape<D> | undefined>
await client.getRow(Todo, key): Promise<Row<MessageShape<D>> | undefined>
await client.scan(Todo):        Promise<RowSet<MessageShape<D>>>
await client.query(Todo, q, { include? }): Promise<RowSet<MessageShape<D>>>
await client.queryServer(Todo, q):         Promise<RowSet<MessageShape<D>>>  // server push-down; NOT merged with overlay

// Entity facade (identical semantics)
await client.db.todo.get(key)
await client.db.todo.getRow(key)
await client.db.todo.scan()
await client.db.todo.query(q, { include? })
await client.db.todo.queryServer(q)

// Cross-workspace
await client.scanAll(Todo):                              Promise<RowSet<MessageShape<D>>>
await client.queryAll(Todo, q, { include?, workspaces? }): Promise<RowSet<MessageShape<D>>>
```

Read behaviors:

- `queryAll` scans all workspaces held by the client unless `workspaces` specifies an explicit list.
  An empty `workspaces` array returns no rows.
- Cross-workspace reads use secondary indexes for any number of workspaces, with or without the `workspaces` option.
  Without the option, the read covers every workspace holding rows of the entity in the local store.
- In place of `where`, a cross-workspace query can provide `byWorkspace: (workspaceId) => where | undefined` to assign distinct predicates per workspace.
  Returning `undefined` excludes that workspace.
  `orderBy`, `limit`, `offset`, and `total` remain shared across all workspaces.
- Every result array is a `RowSet`, an array with non-enumerable properties:

```ts
type RowSet<T> = Row<T>[] & {
	readonly coverage: Coverage;
	readonly total: number; // undefined when the query set total: false
	readonly startCursor?: QueryCursor; // position of first row; absent when empty
	readonly endCursor?: QueryCursor; // position of last row
	readonly hasMore: boolean; // matches remain past limit
};
```

`total` counts matching records before page slicing.
Passing `total: false` in the query disables counting, returning `undefined` for `total`.
`startCursor` and `endCursor` are base64url-encoded `QueryCursor` tokens.
Cross-workspace reads (`queryAll`, `scanAll`) do not generate cursors.

### Grouped reads: counts and distinct values

Grouped reads compute group aggregations over matching rows:

```ts
await client.groupCount(Issue, q):  Promise<Group[]>
await client.distinct(Issue, q):    Promise<(Scalar | undefined)[]>
await client.groupCountServer(Issue, q): Promise<Group[]>   // server push-down
await client.distinctServer(Issue, q):   Promise<(Scalar | undefined)[]>

// Entity facade (identical semantics)
await client.db.issue.groupCount(q)
await client.db.issue.distinct(q)
await client.db.issue.groupCountServer(q)
await client.db.issue.distinctServer(q)

interface GroupQuery<M> {
	by: (i: QueryRef<M>) => GroupKeyRef; // field to partition by, named without an operator
	where?: (i: QueryRef<M>) => Cond;
	limit?: number; // maximum groups, counted from first in value order
}

interface Group {
	value: Scalar | undefined; // undefined represents rows with no value for the field
	count: number; // 0 from distinct
}
```

Example grouped query:

```ts
const byStatus = await client.db.issue.groupCount({
	by: (i) => i.status,
	where: (i) => i.projectId.eq(projectId),
});

const assignees = await client.db.issue.distinct({ by: (i) => i.assignee.id });
```

Grouping rules:

- `by` accepts any comparable scalar field or repeated scalar.
  Sub-messages, maps, raw bytes, and repeated message fields throw an error.
- Returns one group per distinct value, plus one group with `value: undefined` for rows where the path resolves to nothing.
- Groups are sorted in ascending value order, with `NaN` and the no-value group placed last.
- Repeated scalar paths group per element: a row holding three elements joins three distinct groups.
- Only values present on stored rows produce groups.
- A [declared index](proto-options.md#secondary-indexes) matching `[eq-filter fields..., groupPath, ...]` executes as a skip-scan.
  Without an index, the read falls back to scanning and bucketing. See [Grouped reads](../explanation/grouped-reads.md).
- `groupCountServer` and `distinctServer` push grouped reads to the server.
  They are unavailable inside `transact`.

### Grouped reads: first rows of each group

`groupRows` partitions matching rows and returns the leading rows of each group:

```ts
await client.groupRows(Issue, q):       Promise<RowGroup<Issue>[]>
await client.groupRowsServer(Issue, q): Promise<RowGroup<Issue>[]>  // server push-down

// Entity facade (identical semantics)
await client.db.issue.groupRows(q)
await client.db.issue.groupRowsServer(q)

interface GroupRowsQuery<M> {
	by: (i: QueryRef<M>) => GroupKeyRef;
	where?: (i: QueryRef<M>) => Cond;
	orderBy?: (i: QueryRef<M>) => OrderKey[];
	perGroup: number;
	limit?: number;
	total?: boolean; // default true
}

interface RowGroup<T> {
	value: Scalar | undefined;
	rows: Row<T>[];
	total: number | undefined;
	hasMore: boolean;
}
```

Within each group, rows sort by `orderBy`, breaking ties by row key ascending.
Setting `total: false` omits group totals.
`groupRowsServer` pushes the read to the server under `queryServer` rules.

### `Row<T>`

The `Row<T>` interface represents a stored row:

```ts
interface Row<T = unknown> {
	key: string;
	value: T;
	workspaceId: string;
	version: bigint; // server-confirmed version; 0n if optimistic-only
	hidden?: readonly string[]; // fields a field guard hides from the caller
}
```

- `get` returns the decoded `value`.
- `getRow` returns the full `Row` including `version`.
- `version` contains the confirmed server version, or `0n` for optimistic-only rows.
  Pass this value as `expectedVersion` for compare-and-swap writes.
- `workspaceId` identifies the originating workspace in cross-workspace results.
- `hidden` names, by accessor name with a oneof by its own name, the fields the server did not send because a [field guard](proto-options.md#who-reads-a-guarded-field) hides them from the caller. Each reads as its zero value in `value`. The property is absent when the caller reads every field. It follows the server-confirmed row, so a local write does not remove a name from it, and a watcher receives a new `Row` when it changes.
- A text handle on a hidden field rejects with an `Error` when it opens.
- `queryServer` queries the server directly without merging local overlay state.
  The client does not cache results locally, and results do not update watchers.
  The method is unavailable inside `transact`.
  A [secondary index](proto-options.md#secondary-indexes) serves server queries when one matches the predicate and ordering.

## Writes and mutations

Mutations apply optimistically to local storage and enter the background push queue:

```ts
// Schema-first (client.mutate)
await client.mutate.put(Todo, value, opts?)         // opts: CasOptions
await client.mutate.patch(Todo, key, init, opts?)   // opts: PatchOptions
await client.mutate.update(Todo, key, recipe, opts?) // opts: UpdateOptions
await client.mutate.del(Todo, key, opts?)           // opts: CasOptions
await client.mutate.custom(name, argsSchema?, args?)  // server-side custom mutator
client.mutate.list(Todo, rowKey, field): ListHandle  // movable ordered list

// Entity facade (client.db.<entity>.mutate)
await client.db.todo.mutate.create(value, opts?): Promise<string>  // mints ID, returns key
await client.db.todo.mutate.put(value, opts?)
await client.db.todo.mutate.patch(key, init, opts?)
await client.db.todo.mutate.update(key, recipe, opts?)
await client.db.todo.mutate.del(key, opts?)
```

Write methods:

- `put`: Replaces the entire row.
- `patch`: Merges changed fields using a field mask with per-field last-writer-wins resolution.
  Does not create rows; patching an absent row produces no local change and rejects with `conflict`.
- `update`: Reads the row, passes a draft to `recipe`, and patches fields modified by the recipe.
- `create`: Facade-only method. Performs a `put` with an auto-generated time-ordered ID when the key is omitted or empty.
- `custom`: Invokes a server-registered mutator by name. Clears undo and redo stacks. Not available on the entity facade, and transactions cannot batch it.

### Which write verb

Select the write method based on the operation:

| Scenario                           | Method   |
| ---------------------------------- | -------- |
| Writing an entire row              | `put`    |
| Updating specific known fields     | `patch`  |
| Modifying an existing row in place | `update` |

`patch` accepts only the subset of fields being modified.
Passing a full message object to `patch` is a TypeScript compile error:

```ts
const t = await client.db.todo.get(id);
t.title = 'x';
await client.db.todo.mutate.patch(id, t); // compile error
```

### Read-modify-write: update

```ts
await client.db.todo.mutate.update(id, (t) => {
	t.title = 'x';
	t.completed = true;
});
```

Behavior rules:

- When `recipe` modifies no fields, the client queues no mutation.
- When the target row is absent, `update` throws an error.
- The `recipe` function must execute synchronously. Modifying key fields throws an error.
- Inside `transact`, the draft reflects row state before the batch began.

To copy specific fields from another message, supply an explicit field mask:

```ts
await client.db.todo.mutate.patch(id, otherRow, { paths: [ 'title' ] });
```

Every write validates its row key before modifying local storage.
Invalid keys throw an error. See [Naming rules](naming.md).

### Compare-and-swap

Write operations accept version options for optimistic concurrency control:

```ts
interface CasOptions {
	expectedVersion?: bigint; // 0n means "only if row absent"
}
interface PatchOptions extends CasOptions {
	paths?: string[];
}
type UpdateOptions = CasOptions;
```

When `expectedVersion` is set, the server applies the write only if the current row version matches.
Version mismatches reject with code `conflict`, rolling back the optimistic edit and invoking `onMutationRejected`.
Read the base version from `Row.version`.

### Movable ordered lists: ListHandle

`client.mutate.list(schema, rowKey, field)` returns a `ListHandle` for repeated message fields declaring an `order_field`:

```ts
interface ListHandle {
	insertAt(index: number, itemInit: Record<string, unknown>): Promise<string>;
	insertBefore(
		beforeId: string,
		itemInit: Record<string, unknown>,
	): Promise<string>;
	move(id: string, toIndex: number): Promise<void>;
	remove(id: string): Promise<void>;
}
```

### Transactions: client.transact

`client.transact` executes multiple CRUD writes atomically:

```ts
await client.transact(async (tx) => {
	await tx.db.todo.mutate.put({ id, title: 'a' });
	await tx.mutate.patch(Todo, otherKey, { done: true });
});
```

Behavior rules:

- Writes made through `tx` collect into a single `BatchMutation`.
- Reads on `tx` see pre-batch state. Buffered writes are invisible until the callback resolves.
- If the callback throws, the client discards the entire batch.
- Scoping is lexical by handle: the batch does not capture writes issued on `client.mutate.*` during an await.
- `tx` supports read operations, `tx.mutate` (CRUD operations only), and `tx.db`. Transactions do not support custom mutators.

## Queries

A `Query<M>` defines filtering, ordering, and pagination parameters:

```ts
interface Query<M> {
	where?: (w: QueryRef<M>) => Cond;
	orderBy?: (w: QueryRef<M>) => OrderKey[];
	limit?: number;
	offset?: number;
	after?: QueryCursor;
	before?: QueryCursor;
	total?: boolean; // default true; false skips match counting
	facets?: Record<string, FacetSpec<M>>; // per-value counts of the match set
}
```

The following example queries todos:

```ts
import { and, not, or } from 'syncplane';

const rows = await client.db.todo.query({
	where: (t) => and(t.done.eq(false), t.title.contains('milk')),
	orderBy: (t) => [ t.createdAt.desc() ],
	limit: 20,
});
```

### Facets

The `facets` option computes per-value counts of the match set along specified axes:

```ts
interface FacetAxis<M> {
	by: (w: QueryRef<M>) => GroupKeyRef;
	perWorkspace?: boolean; // default true
}

type FacetSpec<M> = ((w: QueryRef<M>) => GroupKeyRef) | FacetAxis<M>;

interface FacetGroup {
	value: Scalar | undefined; // undefined for rows with no value at the path
	count: number; // matching rows; never 0
	workspaceId?: string; // absent when perWorkspace is false
}
```

The following example queries tasks with facets:

```ts
const tasks = await client.db.task.query({
	where: (t) => t.completed.eq(false),
	facets: {
		area: (t) => t.area.id,
		status: { by: (t) => t.status, perWorkspace: false },
	},
});
tasks.facets.status; // [{ value: 'open', count: 12 }, { value: 'blocked', count: 2 }]
```

Facet behavior rules:

- **Methods**: `facets` is supported by `query`, `queryAll`, `queryServer`, `watchQuery`, `watchQueryAll`, and their React hooks (`useQuery`, `useSuspenseQuery`, `useQueryAll`).
- **Result property**: The result's `facets` property contains an array of `FacetGroup` objects for each axis key specified in the query. `facets` is `undefined` when the query specifies no facet axes.
- **Type inference**: The TypeScript type of `facets` matches the keys declared on the query. Accessing an undeclared axis key is a compile-time type error.
- **Accepted fields**: `by` accepts any comparable scalar field or repeated scalar (`GroupKeyRef`). Specifying sub-messages, maps, raw bytes, or repeated message fields throws an error at query time.
- **Count scope**: Counts include all rows admitted by `where` and cursor bounds (`after`, `before`), regardless of `offset` and `limit`. Counts sum to `total`, except on repeated scalar fields, which count once per element.
- **Missing and empty values**: Values held by no matching rows are omitted rather than reported with a count of `0`. Rows with no value at the target path form a group with `value: undefined`.
- **Group ordering**: Groups sort in ascending value order, placing the no-value group (`value: undefined`) last. For multi-workspace queries, ties between workspaces break by workspace ID ascending.
- **Workspaces**: When `perWorkspace` is `true` (default), groups partition by workspace and include `workspaceId`. When `perWorkspace` is `false`, counts aggregate across all queried workspaces and omit `workspaceId`.
- **Execution costs**:
  - In one-shot queries (`query`, `queryAll`, `queryServer`), computing facets reads all matching rows rather than only the paged limit.
  - In live queries (`watchQuery`, `watchQueryAll`, and hooks), counts update incrementally from changed rows on each commit without additional reads.
- **Server limit**: The server rejects requests that specify more facet axes than the server limit `MaxQueryFacets`.

### Paging: after and before

`after` and `before` supply exclusive cursor bounds:

```ts
const page = await client.db.todo.query({
	orderBy: (t) => [ t.createdAt.desc() ],
	limit: 50,
});
const next = await client.db.todo.query({
	orderBy: (t) => [ t.createdAt.desc() ],
	limit: 50,
	after: page.endCursor,
});
const prev = await client.db.todo.query({
	orderBy: (t) => [ t.createdAt.desc() ],
	limit: 50,
	before: page.startCursor,
});
```

Pagination rules:

- Setting `before` alone counts backward from the cursor to retrieve the preceding page.
- Setting both `after` and `before` pages forward within the bounded range.
- Cursors are bound to the `(entity, orderBy)` combination that generated them. Using a cursor with a different entity or ordering throws `CursorShapeError`.
- `query` and `queryServer` accept the same cursors.
- Setting `offset` alongside `after` or `before` throws an error.
- `queryAll` and `watchQueryAll` do not support cursors.

### Operator surface by field type

The query DSL maps fields to operators based on type:

| Field type                                      | Operators                                                                                           |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `string` (`StringOps`)                          | `eq`, `neq`, `in`, `startsWith`, `endsWith`, `contains`, `isSet`, `isNotSet`, `.asc()`, `.desc()`   |
| `number` / `bigint` (`NumericOps`)              | `eq`, `neq`, `in`, `gt`, `gte`, `lt`, `lte`, `isSet`, `isNotSet`, `.asc()`, `.desc()`               |
| `boolean` (`BoolOps`)                           | `eq`, `neq`, `isSet`, `isNotSet`, `.asc()`, `.desc()`                                               |
| `Uint8Array` / singular message (`PresenceOps`) | `isSet`, `isNotSet`                                                                                 |
| repeated scalar (`RepeatedScalarOps`)           | `has`, `hasAny`, `hasAll`, `isEmpty`                                                                |
| repeated message (`RepeatedMessageOps`)         | `any(cb)`, `isEmpty`, `matches(cb)` (references only)                                               |
| singular message                                | Field navigation (`t.settings.color.eq(...)`), `isSet`, `isNotSet`, `matches(cb)` (references only) |
| oneof group (`OneofRef`)                        | Group member operators, `isSet`, `isNotSet`                                                         |
| map (`MapOps`)                                  | `at(key)`, `any(cb)`, `isEmpty`                                                                     |
| Query root (`EntityQueryOps`)                   | `match(query)`, `relevance()` (in `orderBy`)                                                        |

Boolean combinator exports: `and(...conds)`, `or(...conds)`, and `not(cond)`.

#### Full-text match: match and relevance

`match(query)` evaluates to `true` when all terms in `query` appear in [declared search fields](proto-options.md#full-text-search).
The search is case-insensitive and accent-insensitive, with the final term matching as a prefix.
An empty search string matches all rows.
`relevance()` orders results by match score and requires exactly one `match` clause in `where`.

```ts
await client.db.article.query({
	where: (a) => and(a.match('milk cow'), a.state.eq('published')),
	orderBy: (a) => [ a.relevance() ],
	limit: 20,
});
```

Query restrictions:

- Calling `match` on an entity without search fields throws an error.
- Calling `match` outside the query root (such as inside `any()`) throws an error.
- Calling `relevance()` without `match`, with multiple `match` calls, or with an explicit path argument throws an error.
- See [Search data](../how-to/search.md) and [Search model](../explanation/search.md).

#### Asking about one entry of a map

`at(key)` selects a specific map entry and exposes the value type operators:

```ts
await client.db.issue.query({ where: (i) => i.attrs.at('color').eq('red') });
await client.db.issue.query({
	where: (i) => i.custom.at('priority').kind.number.gt(3),
});
await client.db.issue.query({
	where: (i) => i.custom.at('team').kind.text.eq('platform'),
});
await client.db.issue.query({
	where: (i) => i.custom.at('labels').kind.tags.values.hasAny([ 'ui', 'bug' ]),
});
```

When map values use oneofs, queries name the variant through the oneof group.
See [proto options](proto-options.md#maps-and-user-defined-fields).
`isSet()` on `at(key)` tests whether the entry exists.
`isEmpty()` tests whether the map contains zero entries.

#### Asking about ANY entry of a map

`any(cb)` evaluates a predicate across all map entries.
The callback receives a `MapEntryRef` containing `key` and `value`:

```ts
await client.db.issue.query({
	where: (i) => i.custom.any((e) => e.value.kind.number.eq(5)),
});
await client.db.issue.query({
	where: (i) =>
		i.custom.any((e) =>
			and(e.key.startsWith('cf_'), e.value.kind.checked.eq(true))
		),
});
await client.db.issue.query({
	where: (i) => i.custom.any((e) => e.key.in([ 'priority', 'severity' ])),
});
```

Map keys in `any()` use `MapKeyOps`.
Queries do not return which entry matched.

#### Asking about the elements of a message list

`any(cb)` evaluates a condition over individual elements of a repeated message field:

```ts
await client.db.issue.query({
	where: (i) => i.assignees.any((a) => a.id.eq('maya')),
});

await client.db.order.query({
	where: (o) => o.lineItems.any((li) => and(li.sku.eq('A-1'), li.qty.gt(3))),
});
```

For repeated references, `any()` accesses only the stored reference stubs (target key fields).

#### Crossing a reference: matches()

`matches(cb)` performs a semi-join across a forward reference, checking if the referenced row exists and satisfies the callback condition:

```ts
await client.db.issue.query({
	where: (i) => i.assignee.matches((a) => a.teamId.eq(teamId)),
});
await client.db.task.query({
	where: (t) => t.blockedBy.matches((b) => b.done.eq(false)),
});
```

Behavior rules:

- Unset references, dangling stubs, and deleted targets evaluate to `false`.
- A stub resolves in the holder's workspace, unless it is a [workspace-qualified reference](#workspace-qualified-references) naming another one.
- Nested `matches` calls, `matches` inside `any()`, `matches` in subscription predicates, and `matches` on non-reference message fields throw an error at query compilation.
- `matches` names the reference field itself. On a workspace-qualified reference, `t.assignee.user.matches(...)` throws: the entity field inside the wrapper knows nothing of the workspace stored beside it.
- On a [polymorphic reference](#polymorphic-references), `matches` must name a single case (for example, `t.parent.client.matches(...)`, `m.owner.owner.client.matches(...)`, or `m.mentions.owner.client.matches(...)`). Calling `matches` on the `oneof` group or wrapper message throws an error because cases target different entities.
- Reverse relations do not support `matches`.
- See [what a live matches costs](../explanation/relational-queries.md#what-a-live-matches-costs).

### Rejected and surprising cases

The query engine uses consistent ordering and type semantics across client and server:

- **Type safety**: Predicates on unsupported field types (such as comparing or ordering by bytes, messages, maps, or repeated fields) fail at query compilation time and indicate the valid operator to use.
- **Type mismatches**: Type mismatches between literals and fields evaluate to `false`.
- **Empty lists**: An empty or unset list matches no elements. The `any()` operator evaluates to `false`, and `not(...any())` evaluates to `true`.
- **Field presence**: `isSet` on proto fields with implicit presence (scalars, lists, maps) always evaluates to `true`.
- **Oneofs**: Oneof groups are filtered via their member fields (`w.choice.choiceText.eq('x')`). See [proto options](proto-options.md#oneofs).
- **`NaN` comparisons**: The `gt`, `gte`, `lt`, and `lte` operators return `false` when comparing against `NaN`, matching the behavior of `eq`. Ordering ranks values in the order `real < NaN < missing`. Both `NaN` and missing values sort at the end under both `asc` and `desc` directions.
- **Collation**: String ordering uses UTF-16 collation across client and server environments.
- **Collaborative text**: Collaborative text fields query against their visible string representation (for example, `where: (d) => d.body.contains('word')`).
- **Lone surrogates**: The server converts a string with a lone surrogate to U+FFFD.

### Underlying AST types (exported)

The client exports the following Abstract Syntax Tree types:

```ts
type Scalar = string | number | bigint | boolean;
type Path = string[];
type CmpOp = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte';
type StrOp = 'startsWith' | 'endsWith' | 'contains';
type Cond = /* discriminated union: cmp | in | str | rep | ... */;
interface OrderKey { path: Path; dir: 'asc' | 'desc'; }
interface CompiledQuery {
	where?: Cond;
	orderBy?: OrderKey[];
	limit?: number;
	offset?: number;
	after?: QueryCursor;
	before?: QueryCursor;
}
type QueryCursor = string & { readonly __queryCursor: unique symbol };
```

Exported error class: `CursorShapeError`.

## Watchers

Watchers maintain reactive query subscriptions in memory, updating results as local commits occur:

```ts
type WatchCallback<T> = (rows: RowSet<T>, delta: WatchDelta<T>) => void;

interface WatchHandle {
	close(): void;
	setWindow(window: { offset?: number; limit?: number }): void;
}

const view = client.watch(Todo, (rows) => {});
const view = client.watchQuery(Todo, q, (rows) => {}, { include, window });

// Entity facade
const view = client.db.todo.watch((rows) => {});
const view = client.db.todo.watchQuery(q, (rows) => {}, { include, window });
```

Behavior rules:

- `watch` and `watchQuery` return a `WatchHandle` synchronously.
- `close()` is idempotent.
- Callbacks fire only when the delivered result set changes.
- Callbacks must not mutate received row objects.
- Commits in other browser tabs trigger watchers across tabs via `BroadcastChannel`.

### Watching groups: watchGroupCount and watchDistinct

```ts
const view = client.watchGroupCount(Issue, q, (groups) => {}, { onError });
const view = client.watchDistinct(Issue, q, (values) => {}, { onError });

// Entity facade
const view = client.db.issue.watchGroupCount(q, (groups) => {});
const view = client.db.issue.watchDistinct(q, (values) => {});
```

Group watchers re-execute the aggregation query on every commit touching the entity, firing only when output values change.
They do not support windows or coverage reporting.

### Watching group heads: watchGroupRows

```ts
const view = client.watchGroupRows(Issue, q, (groups) => {}, { onError });

// Entity facade
const view = client.db.issue.watchGroupRows(q, (groups) => {});
```

Re-executes per commit and fires when groups, member rows, totals, or `hasMore` flags change.
The watcher decodes returned rows independently per read.

### Watching rows by key: watchRows

```ts
type KeyedRows<T> = ReadonlyMap<string, Row<T> | undefined>;
type KeyedWatchCallback<T> = (rows: KeyedRows<T>, coverage: Coverage) => void;

interface KeyedWatchHandle {
	close(): void;
	setKeys(keys: Iterable<string>): void;
}

const view = client.watchRows(Todo, [ 't1', 't2' ], (rows) => {}, {
	include,
	onError,
});
view.setKeys([ 't2', 't3' ]);
```

- Performs point lookups for specified keys without scanning entire entities.
- Callbacks emit a map of changed keys to their new `Row` or `undefined`.
- The initial callback delivers all requested keys.
- `setKeys` updates the watched set, reading newly added keys.

#### Watching rows across workspaces — `watchRowsAll`

```ts
interface RowRef {
	workspaceId: string;
	key: string;
}
type ScopedKeyedRow<T> = RowRef & { row: Row<T> | undefined };
type ScopedKeyedRows<T> = readonly ScopedKeyedRow<T>[];
type ScopedKeyedWatchCallback<T> = (
	rows: ScopedKeyedRows<T>,
	coverage: Coverage,
) => void;

interface ScopedKeyedWatchHandle {
	close(): void;
	setRefs(refs: Iterable<RowRef>): void;
}

const refs = [ { workspaceId: 'personal', key: 't1' }, {
	workspaceId: 'work',
	key: 't9',
} ];
const view = client.watchRowsAll(Todo, refs, (rows) => {}, {
	include,
	onError,
});
view.setRefs([ ...refs, { workspaceId: 'work', key: 't10' } ]);
```

Tracks specific rows across multiple workspaces.
Emits arrays of `ScopedKeyedRow` entries representing changed rows.

### Paging a live result: setWindow

The `window` option (`{ offset?: number; limit?: number }`) slices the maintained result set:

```ts
const view = client.db.todo.watchQuery(
	{ orderBy: (t) => [ t.createdAtMs.desc() ] },
	(rows) => render(rows, rows.total),
	{ window: { offset: 0, limit: 50 } },
);
view.setWindow({ offset: 50, limit: 50 });
```

`setWindow` repositions the viewport over the existing cached matches without re-executing queries.
`setWindow({})` delivers the full result set.

### Deltas

The second argument to `WatchCallback` provides an array change descriptor:

```ts
type WatchDelta<T> =
	| { kind: 'reset' }
	| { kind: 'splice'; changes: readonly RowSplice<T>[] };

interface RowSplice<T> {
	op: 'insert' | 'remove' | 'update';
	index: number;
	row: Row<T>;
}
```

A delta of kind `reset` indicates that the consumer must replace the full array.
`reset` occurs on initial delivery, window adjustments, entity rescans, and updates to queries with an `offset` or `limit`.

## References and hydration

Reference fields store the primary key of the target entity.
Target rows resolve on read.
Missing target rows resolve to `undefined` or empty arrays without throwing errors.

### Hydration with include

`query` and `watchQuery` accept an `include` tree to hydrate linked records into `refs`:

```ts
interface Include {
	[relation: string]: true | Include | unknown;
	$where?: (w: unknown) => Cond;
	$orderBy?: (w: unknown) => OrderKey[];
	$limit?: number;
	$offset?: number;
}

interface HydratedRow<T> extends Row<T> {
	refs: Record<
		string,
		HydratedRow<unknown> | HydratedRow<unknown>[] | undefined
	>;
}

const rows = await client.db.task.query(
	{ where: (t) => t.done.eq(false) },
	{ include: { project: true, blockedBy: { project: true } } },
);
```

Hydration rules:

- Keys prefixed with `$` represent options applied to relations that resolve to lists.
- `$where`, `$orderBy`, `$limit`, and `$offset` apply per holder row.
- Providing `$` options on a to-one reference or at the root of the `include` tree throws an error.
- Forward references cost one batched read per relation and target workspace, deduplicated across the whole hydration. Reverse relations execute one query per workspace.

### Explicit reference accessors

```ts
// Forward references
client.db.task.refs.project.load(holder):   Promise<Row<Project> | undefined>
client.db.task.refs.blockedBy.load(holder): Promise<Row<Task>[]>

// Reverse relations
client.db.project.refs.tasksByProject(target, q?): Promise<Row[]>

// Schema-first equivalents
client.resolveRef(holderSchema, fieldLocalName, holder)
client.resolveReverseRef(targetSchema, relationName, target, q?)
```

Exported types: `EntityRefs<D>`, `ForwardRefAccessor`, `ReverseRelAccessor`.

A reverse relation takes its name from the reference field that it reverses: the name `.reverseRelation()` sets (`reverse_relation` in a `.proto`), else `<plural(holder entity)>By<Field>` in camelCase, such as `tasksByProject` for the `project` field of `task`. Adding a reference never renames an existing reverse relation. See [Reverse relations](proto-options.md#reverse-relations).

### Workspace-qualified references

A reference resolves in the workspace of the row that holds it. A **reference message** stores the target's workspace beside its key, so a row in one workspace can name a row in another. In a hand-written `.proto`, declare the message with `(syncplane.v1.reference)` (see [proto options](proto-options.md#syncplanev1reference--workspace-qualified-references)). The schema builder synthesizes it:

```ts
const person = schema.entity('person', {
	id: schema.text().key(),
	name: schema.text(),
});
const task = schema.entity('task', {
	id: schema.text().key(),
	assignee: schema.workspaceRef(() => person), // person.refs.tasksByAssignee
	watchers: schema.workspaceRefList(() => person),
});

interface WorkspaceRef<T> {
	workspaceId: string; // empty means the holder's own workspace
	value?: T;
}
```

Write the stub with the workspace it points into. A hand-written reference message uses its own field names:

```ts
await client.ws('proj-1').db.task.mutate.create({
	id: 't1',
	assignee: { workspaceId: 'acme-org', value: { id: 'u1' } },
});
```

A workspace that fails the [workspace-id rule](naming.md#workspace-ids) is refused when the mutation is built, and by the server.

The types cannot see proto options, so `matches()` callbacks and `refs.<name>.load()` recognize a hand-written reference message by its shape: one message field, alone or beside a string field named `workspace_id` (`workspaceId` in TypeScript). A workspace field with another name leaves those typed as the wrapper, while they still resolve to the target at runtime.

Resolution rules:

- Forward accessors, `include`, and `resolveRef` resolve each stub in the workspace it names, from the workspaces the client holds. A target the client does not hold resolves to `undefined`, like any orphan.
- Reverse relations from a target read the source entity across every held workspace and match the workspace as well as the key. A row with the same key in another workspace is a different row.
- `matches()` evaluates the target in the workspace the stub names. Its coverage `relation` bound clears only when the target entity is complete in every held workspace; a `follow` spec does not cover it.
- `t.assignee.eq(id)` compares the key; `t.assignee.workspaceId.eq(ws)` compares the workspace. Any other field of the stub is rejected at query time.
- Watchers that hydrate or cross such a reference re-fire on a commit to the target entity in any workspace.
- `queryServer`, server-side `include`, and `follow` resolve inside one workspace: a stub naming another workspace matches nothing there.
- A reference message is replaced whole on every write. A `workspaceRefList` merges as a set: an `update` that adds or removes elements writes only those, identified by `workspaceId` and key together, so concurrent adds and removes from different clients all land. Wrap each reference in a `keyedList` element when an element must carry more than the reference.

### Polymorphic references

A **polymorphic reference** targets one of several possible entities. In `.proto` schemas, declare a polymorphic reference as a `oneof` of reference fields on the holder entity, or as a reference message containing a `oneof` alongside an optional workspace field. See [proto options](proto-options.md#polymorphic-references). Because the TypeScript schema builder does not support `oneof` declarations, declare polymorphic references in `.proto` files.

```proto
message Task {
	option (syncplane.v1.entity) = { name: "task" key_fields: [ "id" ] };
	string id = 1;
	oneof parent {
		Client client = 2;
		AreaRef area = 3;
	}
}
```

The `oneof` defines the relation name, and each reference member defines a **case**:

```ts
// Write and switch the case: the mask names the group, as for any oneof.
await client.db.task.mutate.put({
	id: 't1',
	parent: { case: 'client', value: { id: 'c1' } },
});
await client.db.task.mutate.patch('t1', {
	parent: { case: 'area', value: { area: { id: 'a1' } } },
});

// Resolve whichever case the row holds; the row type is the union of the targets.
const parent = await client.db.task.refs.parent.load(task); // Row<Client | Area> | undefined
if (parent?.value.$typeName === 'example.v1.Client') { /* … */ }

// Hydrate and follow by the oneof's name.
await client.db.task.query({}, { include: { parent: true } });
subscription(TaskSchema, undefined, { include: { parent: true } });

// Query one case.
await client.db.task.query({
	where: (t) => t.parent.client.matches((c) => c.name.eq('Acme')),
});
await client.db.task.query({ where: (t) => t.parent.client.id.eq('c1') });
await client.db.task.query({ where: (t) => t.parent.area.isSet() });
```

Rules:

- **Relation accessors**: The forward accessor uses the `oneof` group name on the holder (`refs.parent`, `include: { parent }`) or the field name on a reference message (`refs.owner`). Individual cases do not expose separate accessors on the holder.
- **Reverse relations**: Every case target receives a reverse relation accessor. A member of the holder's `oneof` is its own field and takes its own name (`client.db.client.refs.tasksByClient(client)` and `client.db.area.refs.tasksByArea(area)`). A reference message field gives its name to every case (`refs.tasksByOwner` on each target). See [Reverse relations](proto-options.md#reverse-relations).
- **Resolution**: `refs.<relation>.load()` and `include` resolve the active case stored on the row within that case's target entity and designated workspace. In to-many polymorphic references, each element resolves its own case, allowing the list to contain rows from different entities.
- **Include options**: Specifying list options (`$where`, `$orderBy`, `$limit`, `$offset`) on a polymorphic list relation throws an error because the options target a single entity type, but the relation resolves to multiple entity types.
- **Set merging**: Repeated polymorphic reference messages merge as a set keyed by `<workspace>/<case>/<key>`. Mutation updates add or remove only the modified elements.
- **Type inference**: TypeScript types resolve a reference message containing a `oneof` of message fields (with or without `workspaceId`) to the union of its case message types.

## Undo and redo

Configure in-memory undo and redo stacks through the `undo` client option:

```ts
interface UndoOptions {
	enabled?: boolean;
	maxDepth?: number;
}
```

Undo and redo methods:

```ts
await client.undo(): Promise<boolean>
await client.redo(): Promise<boolean>
client.canUndo(): boolean
client.canRedo(): boolean
client.onUndoChange(cb: () => void): () => void
```

Undo behaviors:

- `undo()` and `redo()` return `false` when the stack has no actions.
- `put`, `patch`, and `del` record inverted mutations.
- Undoing a `del` restores the row with `expectedVersion: 0n`. When the deleted row had fields the server did not send (`Row.hidden`), the undo rejects with `UndoRefusedError` and the step is used up: a restore would recreate those fields as zero values.
- An undo restores no field the server did not send.
- Transaction batches commit to the undo stack as a single step.
- Custom mutators clear both undo and redo stacks.
- New user edits clear the redo stack.
- Rejected mutations remove their mutation group from the undo history.

### Nested undo scopes

Fork isolated undo stacks for independent interface elements:

```ts
const dialog = client.ws('w1').undoScope({ maxDepth: 200 });

await dialog.db.task.mutate.patch(id, { priority: 2 });
await dialog.undo();

dialog.close();
```

Scope rules:

- Scopes fork from the workspace. Scopes created from another scope do not chain.
- Custom mutations clear all undo stacks across the workspace.
- `close()` clears the scope's stacks without merging them into the parent workspace.
- Calling `undo()` on a closed scope returns `false`.

## Sync status

The client exposes synchronization status through `client.status()` and `onStatusChange`:

```ts
interface SyncStatus {
	state: 'idle' | 'syncing' | 'error';
	online: boolean;
	dirty: boolean;
	pendingMutations: number;
	lastSyncedAt?: Date;
	lastError?: SyncErrorInfo;
	schemaSkew?: SchemaSkewInfo;
	workspaces: Record<string, WorkspaceStatus>;
	scope?: SyncScopeInfo;
	workspaceAccess?: WorkspaceAccess;
	hydration?: HydrationInfo;
}

interface WorkspaceStatus {
	workspaceId: string;
	pendingMutations: number;
	access?: WorkspaceAccess;
	scope?: SyncScopeInfo;
	hydration?: HydrationInfo;
}

type WorkspaceAccess = 'granted' | 'revoked' | 'none';
type SchemaSkewAction = 'ignore' | 'warn' | 'block' | 'resync';

interface SyncScopeInfo {
	own: number;
	effective: number;
	widened: boolean;
}

interface HydrationInfo {
	pending: readonly string[];
	total: number;
	done: number;
}
```

Exported status and rejection constants: `CodePoisonDropped`, `CodeConflict`, `CodeInvalidArgument`, `CodePermissionDenied`, `CodeAccessDenied`.
See [Sync status and failure codes](sync-status-and-codes.md).

### Devtools

Attach observational debugging utilities to the global scope:

```ts
import { attachDevtools } from 'syncplane';

attachDevtools(client, opts?);
```

Global devtools methods (`__syncplane`):

- `__syncplane.snapshot()`: Returns status, lock states, pending mutation queues, coverage details, and active rooms.
- `__syncplane.explain(entity, key, workspaceId?)`: Explains the local presence, overlay state, and visibility verdict for a specific row.
- `__syncplane.rejections()`: Returns buffered rejection records, ordered oldest first.
- `__syncplane.events()`: Returns buffered sync events, ordered oldest first.
- `__syncplane.detach()`: Removes listeners and deletes the global handle.

Options: `eventBufferSize` (default `200`), `rejectionBufferSize` (default `100`), `expose` (global variable name, default `'__syncplane'`; pass `false` to avoid global assignment).

## Partial sync and subscriptions

Subscriptions restrict local synchronization to a subset of entity data:

```ts
import { subscription } from 'syncplane';

const specs = [
	subscription(Task, (t) => t.archived.eq(false), {
		include: { project: true },
		depth: 2,
	}),
	subscription(Issue, (i) => i.state.eq('open'), {
		limit: 100,
		orderBy: (i) => [ i.priority.desc() ],
	}),
	subscription(Comment, { limit: 500, byRecency: true }),
	subscription(Document, { keys: pinnedDocumentIds, include: { author: true } }),
];

const client = new SyncClient({ subscriptions: specs, ... });
client.setSubscriptions(specs);
```

Subscription structures:

```ts
interface SubscriptionSpec {
	schema: DescMessage;
	where?: (w: unknown) => Cond;
	follow?: IncludeSpec;
	followDepth?: number;
	orderBy?: (w: unknown) => OrderKey[];
	limit?: number;
	byRecency?: boolean;
	keys?: readonly string[];
}

interface SubscriptionOptions<D> {
	include?: IncludeSpec;
	depth?: number;
	limit?: number;
	orderBy?: (w: QueryRef<MessageShape<D>>) => OrderKey[];
	byRecency?: boolean;
	keys?: readonly string[];
}

function subscription<D>(
	schema: D,
	where?: (w: QueryRef<MessageShape<D>>) => Cond,
	opts?: SubscriptionOptions<D>,
): SubscriptionSpec;
function subscription<D>(
	schema: D,
	opts: SubscriptionOptions<D>,
): SubscriptionSpec;
```

The second form takes the options in place of the predicate, for a subscription that has none.

Entities declared with [`server_only`](proto-options.md#server-only-entities) must not appear in subscription specifications.
Including a `server_only` entity in a subscription throws an error.

### Windows: capping a subscription by rank

Adding `limit` creates a windowed subscription.
The client synchronizes up to `limit` rows matching `where`, ordered by `orderBy` or write timestamp (`byRecency: true`).
Rows falling outside the window are deleted from local storage.

Validation rules:

- Setting `orderBy` or `byRecency` without `limit` throws an error.
- Supplying a non-positive or non-integer `limit` throws an error.
- Supplying both `orderBy` and `byRecency` throws an error.
- The server rejects window sizes that exceed the server maximum (`MaxWindowSize`, [default 1000](server.md)).
- See [Partial sync and coverage](../explanation/partial-sync-and-coverage.md).

### Key sets: naming a subscription's rows

Adding `keys` creates a key-set subscription.
The client synchronizes exactly the named rows, whatever their fields hold, and nothing else of the entity.
The keys are the row keys `get()` takes.
An empty array names no rows, which differs from omitting `keys` (the whole entity).
`include` walks references from the named rows.

Changing the set delta-resyncs the difference: rows entering the set arrive, rows leaving it are deleted from local storage.
The server reads only the rows named on either side of the change.
Local reads of the entity report the `predicate` coverage bound.

Validation rules:

- Supplying `keys` together with `where`, `limit`, `orderBy` or `byRecency` throws an error.
- Supplying an empty key, or one with a control character, throws an error.
- The server rejects sets that exceed the server maximum (`MaxSubscriptionKeys`, [default 1000](server.md)).

### Holds: one row in scope on demand

`hold(key)` brings one row into scope without changing the declared subscriptions.
It is the way to open a row found by `queryServer`, or by a link, for editing:

```ts
const hold = client.db.document.hold(id); // or client.hold(Document, id)
await hold.ready; // the row is local if the server holds it
const text = await client.db.document.text(id, 'body');
// ...
await text.close();
hold.release();
```

```ts
interface HoldHandle {
	readonly ready: Promise<void>;
	release(): void;
}
```

A held row syncs like a row named by a key-set subscription.
It arrives on the next sync cycle, follows server writes, feeds watchers and text handles, and leaves as a tombstone if it is deleted on the server.
`ready` resolves once a sync cycle has pulled with the row in scope.
It resolves early when the hold is released or the client closes, and never rejects.
A row the server does not hold, or that the caller may not read, stays absent after `ready`.

The row stays in scope until the last hold on it is released, plus `holdGraceMs`.
Holds on one row count: two holds need two releases.
`release()` is idempotent.

Holds do not persist across a reload.
Every tab on the account syncs the union of all tabs' holds, so a row a peer tab holds stays when this tab closes.
See [Partial sync and coverage](../explanation/partial-sync-and-coverage.md#why-holds-exist) for what a hold costs the server.

Rules:

- Under full sync a hold changes nothing.
- For an entity a declared subscription covers whole, a hold adds nothing.
- Local reads of an entity scoped only by holds report the `predicate` coverage bound.
- Supplying an empty key, or one with a control character, throws an error.
- Naming a `server_only` entity throws an error.
- `hold` is not available inside a transaction.
- The server bounds the held rows of one entity with `MaxSubscriptionKeys` ([default 1000](server.md)).

### Coverage

The `Coverage` object describes the completeness of locally stored entity data:

```ts
type CoverageBound =
	| 'unsubscribed'
	| 'predicate'
	| 'window'
	| 'hydrating'
	| 'relation';

interface Coverage {
	complete: boolean;
	bounds: CoverageBound[];
	limit?: number;
}
```

Coverage bounds:

- `unsubscribed`: The entity is not included in active subscriptions.
- `predicate`: The entity is filtered by a subscription predicate or named by a subscription key set.
- `window`: The entity is constrained by a window `limit`.
- `hydrating`: The initial snapshot for the entity is currently downloading.
- `relation`: A reference traversal (`matches()`) encountered targets that may not be present locally.

### RowSet reference

`RowSet<T>` is an array: `.map`, `.length`, spreads, and destructuring behave the same as on a standard array.
The `coverage`, `total`, `facets`, `hasMore`, and cursor properties are non-enumerable, so `toEqual([])`, `Object.keys`, and `JSON.stringify` do not see them.

### Delivery order

The `bootstrapPriority` option specifies entities to prioritize during initial snapshot synchronization:

```ts
new SyncClient({
	bootstrapPriority: [ Issue, Comment ],
	...
});
```

Entities in `bootstrapPriority` arrive first during cold-start synchronization.
Remaining entities follow in server order.

## TypeScript schema builder

The `schema` namespace builds entity schemas and compiles Protocol Buffers descriptors in TypeScript:

```ts
import { schema } from 'syncplane';

const todo = schema.entity('todo', {
	id: schema.text().key(),
	title: schema.text(),
	done: schema.bool(),
	createdAt: schema.int64(),
	tags: schema.list(schema.text()),
	settings: schema.message({ color: schema.text(), size: schema.int32() }),
	blob: schema.bytes().opaque(),
	items: schema.keyedList(
		schema.message({ itemId: schema.text(), pos: schema.text() }),
		'itemId',
	).order('pos'),
});

type Todo = schema.Infer<typeof todo>;

const bundle = schema.defineSchema(todo);
const client = new SyncClient({ entities: bundle.descMessages() });
```

### Field constructors

The `schema` builder provides the following field types:

| Constructor                             | Return type                       | Description                                                                                                                    |
| --------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `schema.text()`                         | `Field<string>`                   | String scalar field.                                                                                                           |
| `schema.bool()`                         | `Field<boolean>`                  | Boolean scalar field.                                                                                                          |
| `schema.int32()`                        | `Field<number>`                   | 32-bit signed integer.                                                                                                         |
| `schema.int64()`                        | `Field<bigint>`                   | 64-bit signed integer.                                                                                                         |
| `schema.uint64()`                       | `Field<bigint>`                   | 64-bit unsigned integer.                                                                                                       |
| `schema.double()`                       | `Field<number>`                   | 64-bit floating point number.                                                                                                  |
| `schema.bytes()`                        | `Field<Uint8Array>`               | Raw byte array. Call `.opaque()` for atomic replacement.                                                                       |
| `schema.list(inner)`                    | `Field<T[]>`                      | Repeated scalar array.                                                                                                         |
| `schema.message(fields)`                | `Field<RowOf<F>>`                 | Nested sub-message with per-field last-writer-wins resolution.                                                                 |
| `schema.ref(() => target)`              | `Field<Infer<E>>`                 | Strongly-typed to-one reference.                                                                                               |
| `schema.refList(() => target)`          | `Field<Infer<E>[]>`               | Strongly-typed to-many reference list.                                                                                         |
| `schema.workspaceRef(() => target)`     | `Field<WorkspaceRef<Infer<E>>>`   | To-one reference that may point into another workspace. See [Workspace-qualified references](#workspace-qualified-references). |
| `schema.workspaceRefList(() => target)` | `Field<WorkspaceRef<Infer<E>>[]>` | To-many form of `workspaceRef`. Merges as a set of (`workspaceId`, key) elements.                                              |
| `schema.keyedList(elem, ...keyFields)`  | `KeyedListField`                  | Repeated message list merged by item key. Supports `.order(posField)`.                                                         |
| `schema.map(keyType, value)`            | `Field<Record<...>>`              | Key-value map.                                                                                                                 |

Field modifiers:

- `.key()`: Marks a scalar field as a primary key. Every entity requires at least one key field. See [Naming rules](naming.md).
- `.opaque()`: Disables sub-field merging; writes replace the field value entirely.
- `.indexed()`: Declares a secondary index on the field.
- `.order(posField)`: Sets the fractional index ordering property on `keyedList`.
- `.reverseRelation(name)`: Names the reverse relation a reference adds to its target. Valid on `ref`, `refList`, `workspaceRef`, and `workspaceRefList`. See [Reverse relations](proto-options.md#reverse-relations).
- `.creator()`: Records who created the row. On a create that is not trusted, the server stores the caller's primary subject in the field, and later writes that are not trusted cannot change it. Valid on a `text()` field that is not a key field.

### Secondary indexes

Declare composite secondary indexes using the `indexes` option:

```ts
const issue = schema.entity('issue', {
	id: schema.text().key(),
	state: schema.text().indexed(),
	rank: schema.int32(),
}, {
	indexes: [ {
		name: 'by_state_rank',
		key: [ { path: 'state' }, { path: 'rank', desc: true } ],
	} ],
});
```

Index constraints:

- Index names must not start with `__` (reserved for internal engine indexes).
- `.indexed()` throws an error when used on message lists, references, maps, or collaborative text fields.
- Paths can traverse nested fields (`'settings.size'`) and repeated message fields (`'lineItems.sku'`).
- Map indexes must use `*` notation (`custom.*.key` or `custom.*.value.number`).
- See [limits and trade-offs](../explanation/limits-and-trade-offs.md) and [Telemetry](telemetry.md#local-reads).

### Full-text search

Configure full-text indexing with `.searchable()` and the entity `search` option:

```ts
const article = schema.entity('article', {
	id: schema.text().key(),
	title: schema.text().searchable(),
	summary: schema.text(),
}, {
	search: { fields: [ 'summary' ], keepDiacritics: false },
});
```

`.searchable()` throws an error on non-string fields.
See [proto options](proto-options.md#full-text-search) and [Search data](../how-to/search.md).

### Access model

The `access`, `grant` and `members` entity options declare who can read and write rows:

```ts
const project = schema.entity('project', { id: schema.text().key() }, {
	access: {
		allow: [
			{ read: true, requires: 'view_projects' },
			{ write: true, requires: 'edit_projects' },
		],
	},
});

const projectMember = schema.entity('project_member', {
	id: schema.text().key(),
	project: schema.ref(() => project),
	userId: schema.text(),
	role: schema.text(),
}, {
	grant: {
		subject: 'userId',
		role: 'role',
		roles: [
			{ name: 'project_reader', grants: [ 'view_projects' ] },
			{
				name: 'project_writer',
				includes: [ 'project_reader' ],
				grants: [ 'edit_projects' ],
			},
		],
	},
	access: {
		parents: [ 'project' ],
		allow: [
			{ read: true, requires: 'view_projects' },
			{ write: true, requires: 'edit_projects' },
		],
	},
});

// Every caller admitted to the workspace reads labels and adds them.
// No rule admits update or delete, so only trusted server-side writes do that.
const label = schema.entity('label', { id: schema.text().key() }, {
	access: {
		allow: [ { read: true, create: true, admitted: true } ],
	},
});
```

Each rule in `access.allow` sets one or more of `read`, `create`, `update`, `delete` and `write`, and exactly one source: `requires: '<name>'` or `requires: [ '<name>', ... ]`, which needs every permission it names, or `admitted: true`. The rules lower to `syncplane.v1.AccessRule`; see [Access model](proto-options.md#access-model) for how they admit each operation.
A rule can also set checks on the row: `callerIs` names a `text()` field by accessor name, `fields` lists accessor names, and `when` is a function that takes the row's field proxy, as a query's `where` does, and returns the condition the row must match, such as `when: (t) => t.archived.eq(false)`. `entity()` throws on a `when` that uses `matches()` or `match()`. `within` holds one entry or a list. Build each entry with `schema.within(target, { callerIs?, when? })`: `callerIs` names a `text()` field of `target` by accessor name, and `when` is a condition on `target`'s fields, such as `within: schema.within(project, { when: (p) => p.archived.eq(false) })`. TypeScript types both against `target`'s fields, and a rule does not take a plain `{ entity, callerIs }` object. `target` can be a thunk, `schema.within(() => project, { ... })`, for an entity declared later; `defineSchema()` calls it. For the rule's own entity, in an entity whose rows sit below rows of their own, write `{ self: true, callerIs?, when? }`, which types `callerIs` and `when` against the entity's own fields. `defineSchema()` throws on an entry whose entity is not in the schema, whose `callerIs` is not a singular `text()` field of it, or that reads a field a guard of that entity can hide. See [Checks on the rows above](proto-options.md#checks-on-the-rows-above).
`grant.subject` and `grant.role` name fields by accessor name. `grant.role` names a `text()` field. `grant.subject` names a `text()` field, or a singular `ref()` to an entity whose `members` option names its member entity, which grants to every member of the team the row names. See [Teams](proto-options.md#teams). `access.parents` names the access parents by accessor name: `ref()` and `refList()` fields, in any order, no name twice; see [Several access parents](proto-options.md#several-access-parents). A `workspaceRef()` cannot be a parent. A grant entity names at most one parent, and not a `refList()`.
`access.guard` lists field guards, each `{ fields, allow }`: `fields` names the fields the guard restricts by accessor name, and `allow` holds rules with the shape of `access.allow` except `delete` and `fields`. A guard with the read rule `{ read: true, admitted: true }` shows its fields to every reader of the row; a narrower read rule, or none, hides them from some readers or from all. A guard lowers to `syncplane.v1.FieldGuard`; see [Field guards](proto-options.md#field-guards). `entity()` throws on a guard that names a key field, a field that `parents` names, a `.creator()` field or a field another guard names, on a guard rule whose `callerIs` or `when` reads a guarded field, and on a guard that can hide a field a rule in `allow` checks with `callerIs` or `when`, or the `grant.subject` or `grant.role` field.
Each role sets `grants`, `includes` or both. `includes` names roles of the same `grant`, and the role grants every permission of each of them.
`entity()` throws on the rules that concern one entity. `defineSchema()` also throws on a parent that references an entity without `access`, a `grant.subject` reference to an entity without `members`, and a `members` option that does not name a grant entity of the schema whose one access parent is the team entity and whose subject is a `text()` field. The server checks the remaining rules that span entities, such as permission coverage and unique role names, when the schema is registered.
TypeScript checks the field names in `parents`, `subject`, `fields`, `grant.subject` and `grant.role` against the entity's fields: `subject` and `grant.role` take a `text()` field, and `grant.subject` a `text()` field or a singular reference. Lists such as `grants`, `includes` and `fields` take `readonly` arrays. To share rules between entities, declare them with `satisfies schema.AccessDecl`, not with an `AccessDecl` annotation, because the annotation takes any name and does not fit an entity's options.

`put`, `patch`, `update` and `del` reject with an `Error` before they change local state when the write can only be an operation that no rule of the entity names: `del` without a delete rule, `patch` and `update` without an update rule, `put` with `expectedVersion: 0n` without a create rule, `put` with another `expectedVersion` without an update rule, and `put` with no `expectedVersion` when no rule names create or update. See [Operations no rule admits](proto-options.md#operations-no-rule-admits).

`patch` and `update` also reject with an `Error` when they name a field whose guard has no rule for `update`, or a `.creator()` field. `put` keeps the local value of such a field in the optimistic row, and the zero value of a guarded field whose guard has no rule for `create` on a new row. The server decides a guard rule that admits only some callers: the client queues the write, and a refused `patch` comes back as a rejection ack.

#### Predicting access

```ts
type CanAnswer = 'yes' | 'no' | 'unknown';
type CanDetail =
	| { answer: 'yes' | 'no'; reason?: undefined }
	| { answer: 'unknown'; reason: UnknownReason };

client.can(schema, key, operation, opts?): Promise<CanAnswer>; // operation: 'read' | 'update' | 'delete'
client.canCreate(schema, row, opts?): Promise<CanAnswer>;
client.hasPermission(schema, key, permission, opts?): Promise<CanAnswer>;
client.can(schema, key, operation, { detail: true }): Promise<CanDetail>; // also canCreate and hasPermission
client.ws(id).can(...); // also canCreate and hasPermission
client.onAccessChange((workspaceId) => { ... }): () => void;
```

Each method predicts from two inputs: the `CallerAccess` summary the server sends when the caller's grants change, and the rows held locally, queued writes included. The client stores the summary with the cursor, so it survives a restart. A prediction walks up a chain of access parents at most as many rows as the server's `MaxAccessDepth`, which the summary carries, or 32 rows when the summary does not carry it.

The answer is advisory. The client does not refuse writes from it: every write is queued, and a write the server refuses comes back as a rejection ack. A row the caller lost access to keeps showing a pending local write until that ack arrives.

##### Operations: `can` and `canCreate`

Use `can` and `canCreate` to decide whether to show a control that reads, creates, changes or deletes rows. They evaluate the entity's access rules as the server does: the permissions of `requires` on the row's chain, and the `callerIs`, `when`, `within` and `fields` checks.

```ts
const canEdit = await client.can(CommentSchema, id, 'update', {
	fields: [ 'body' ],
});
const canDelete = await client.can(CommentSchema, id, 'delete');
const canComment = await client.canCreate(CommentSchema, {
	id: 'c1',
	task: { id: taskId },
});
```

- `can` predicts for the row with `key`. `canCreate` predicts for `row` as the server would store it, with each `.creator()` field holding the caller's primary subject.
- For `'update'`, `fields` names the fields the update changes, by accessor name, with a oneof named by its own name, as a patch init object names them. A rule with a `fields` list counts only when its list names each of them, and each guarded field among them needs a rule of its guard that admits the update. `can` rejects with an `Error` for a name that is not a field of the entity.
- Without `fields`, the answer is for some update: `'yes'` when a rule admits an update of one field its `fields` list names, or a rule without a list admits an update. Use it for an Edit control, and pass `fields` to decide each input of a form.
- For `'read'`, `fields` names the fields to read. Each guarded field among them needs a read rule of its guard that admits the caller on the row, so the answer is `'no'` for a field the server sends hidden. Without `fields`, the answer is for reading the row.
- For an entity that declares no `access`, both answer `'yes'`. Such an entity is open to every caller in the workspace.
- The answer also covers the checks the server makes beside the rules, from the rows the client holds:
  - An update that changes the access parent needs, where the row is now, the right to read the row and each field of it a guard can hide, and the right to read every row of each entity whose rows can sit below it. A read rule with a `callerIs` or `when` check does not give the right to read every row.
  - An update of a field that a `within` entry reads needs the same right to read the rows below.
  - An update of a field that a read rule of a hiding guard checks needs the right to read the guarded fields. The answer is `'unknown'` at best, because the new value decides who reads them after the update.
  - On a grant entity, `canCreate`, a `'delete'`, and an update of the subject, the role or the access parent need every permission of the row's role where the row applies. An update of the role or the access parent answers `'unknown'` at best.
  - For `canCreate`, each permission a rule requires and each of its `within` entries must hold through each access parent of the row on its own: at the row, or at that parent or a row above it. A row below two parents shows to the readers of both, so an entry that holds through one parent only does not admit the create.
  - Where a chain of access parents can lead back to an entity it passes, as a folder in a folder does, a write that adds an access parent needs the right to read that parent. `canCreate` answers `'no'` when the caller does not read a parent the row names, and `'unknown'` (`row_not_held`) when the client does not hold that parent, which can be missing or hidden. An update that changes the access parent answers `'unknown'` at best (`new_value`), because the new parent decides.
  - A `'delete'` answers `'no'` while the client holds a live row whose access parent names the row, or a live grant row whose subject names the row, because the server refuses that delete with `conflict`. To find one, `can` scans the rows held locally of each entity whose rows can sit below the row or grant to its members.
  - An `'update'` or a `'delete'` of a row the client does not hold answers `'unknown'` at best (`row_not_held`), because the server refuses both for a row it does not hold live.
  - When the client registers a grant entity and the stored summary holds no permission, every prediction answers `'no'`, for an entity outside the model too, because the server refuses every request of such a caller in the workspace. This is the state after the caller loses its last grant, while `status().workspaces[id].access` is `'none'`.

| Answer      | When                                                                      |
| ----------- | ------------------------------------------------------------------------- |
| `'yes'`     | At least one rule admits the operation.                                   |
| `'no'`      | No rule admits it.                                                        |
| `'unknown'` | No rule admits it for certain, and one of the cases below decides a rule. |

`can` and `canCreate` answer `'unknown'` in the cases that [Why an answer is unknown](#why-an-answer-is-unknown) lists.

##### Permissions: `hasPermission`

Use `hasPermission` for a permission that no access rule checks, such as the one a custom mutator needs. It predicts whether the caller holds `permission` at the workspace root, at the row itself, or at a row up its chain of access parents. It does not evaluate rules, so a caller can hold a rule's permission and still be refused by its `callerIs`, `when`, `within` or `fields` check. To ask about an operation, use `can`.

```ts
const canInvite = await client.hasPermission(
	ProjectSchema,
	projectId,
	'invite_members',
);
```

| Answer      | When                                                                                                                                                                                                                                                                                                                                                            |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `'yes'`     | The summary holds the permission at the root, the row, or a row up its chain.                                                                                                                                                                                                                                                                                   |
| `'no'`      | It does not, and each path up the chain ends at a row without an access parent or with its reference unset, or at a row the client does not hold where the caller holds the permission at no row of an entity that can sit above it.                                                                                                                            |
| `'unknown'` | No summary has arrived, the answer depends on a row of the chain the client does not hold (the row itself included) and the caller holds the permission at a row of an entity that can sit above it, or on a chain longer than 32 rows, or a local write to a grant row (or a delete of an anchor row, or such a write by a custom mutation) waits for its ack. |

##### Why an answer is unknown

Pass `{ detail: true }` to `can`, `canCreate` or `hasPermission`, or to their hooks, to get a `CanDetail`. Its `reason` says why an answer is `'unknown'`; for `'yes'` and `'no'` it is `undefined`. When more than one reason applies, the answer gives one of them. Equal answers are the same object.

```ts
const { answer, reason } = await client.can(TaskSchema, id, 'delete', {
	detail: true,
});
```

| `reason`           | When                                                                                                                                                                                                                                                            |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `'loading'`        | The hook has no answer yet. Only the hooks give it.                                                                                                                                                                                                             |
| `'no_summary'`     | No summary has arrived for the workspace.                                                                                                                                                                                                                       |
| `'pending_grant'`  | A local write to a grant row or a delete of a row that other rows sit below waits for its ack. That includes such a write by the mirror of a custom mutation, and, in a schema with grant entities, a custom mutation that has no mirror.                       |
| `'row_not_held'`   | A rule's `callerIs` check or condition reads a row the client does not hold, or a permission depends on a row of the chain the client does not hold (the row itself included) and the caller holds that permission at a row of an entity that can sit above it. |
| `'new_value'`      | An update changes a field whose new value decides: the access parent, a field a `callerIs` check reads, or a field that a `when` or `within` check reads.                                                                                                       |
| `'chain_too_deep'` | The chain is longer than the server's `MaxAccessDepth`, or 32 rows when the summary does not carry it.                                                                                                                                                          |
| `'error'`          | The read of the local store failed. Only the hooks give it; the methods reject.                                                                                                                                                                                 |

##### Following changes

`onAccessChange` calls back with the workspace id once for each commit that stores or removes the summary, queues, acks or rolls back a local write to a grant row (or a delete of an anchor row), replays the mirror of a queued custom mutation that writes such a row, or writes a row of an entity that other rows sit below, locally or from the server. It runs for commits by this tab and by other tabs sharing the store, and can run when no answer changed. It does not report a change of the row an answer is for unless that row is of such an entity, so to follow the answer for one row, watch that row too. The React hooks `useCan`, `useCanCreate` and `useHasPermission` do both. It does not report a change of the rows below a row, or of the grant rows that name it as their subject, either, which the answer for a `'delete'` reads; watch them to follow that answer. `useCan` does this for a `'delete'`.

### Schema compilation: defineSchema

```ts
interface SchemaBundle {
	entities: readonly EntitySchema[];
	toFileDescriptorSet(): FileDescriptorSet;
	toBinary(): Uint8Array;
	toRegistry(): FileRegistry;
	descMessages(): DescMessage[];
	descMessage(name: string): DescMessage;
}
```

Functions:

- `schema.defineSchema(...entities)`: Compiles entity definitions into a `SchemaBundle`.
- `schema.entitiesFromDescriptorSet(bytes)`: Reconstructs `DescMessage[]` descriptors from binary FileDescriptorSet bytes.

### Schema registration and administration

```ts
import { fetchSchema, registerSchema } from 'syncplane';

await registerSchema(transport, appId, bundle, { token?, expectedRevision?, allowAccessChange? }): Promise<RegisterResult>;
await fetchSchema(transport, appId): Promise<DescMessage[]>;

interface RegisterResult {
	revision: string;
	status: 'created' | 'updated' | 'unchanged';
	entityNames: string[];
}
```

## Key-value stores

```ts
import {
	DEFAULT_STORAGE_PREFIX,
	IDBKVStore,
	type IndexEntry,
	type KVRead,
	type KVStore,
	type KVWrite,
	MemoryKVStore,
	type StorageNames,
	storageNames,
} from 'syncplane';
```

Built-in storage backends:

- `IDBKVStore`: IndexedDB implementation for browser environments. Uses Web Locks for cross-tab synchronization.
- `MemoryKVStore`: In-memory implementation for Node.js and testing environments.

### Storage names

Storage resource identifiers derive from `storagePrefix` and `accountId`:

| Field           | Namespace        | Default format                    |
| --------------- | ---------------- | --------------------------------- |
| `store`         | IndexedDB        | `sync:<accountId>`                |
| `kvLock`        | Web Locks        | `sync:<accountId>`                |
| `streamLock`    | Web Locks        | `sync:stream:<accountId>`         |
| `changeChannel` | BroadcastChannel | `sync:change:<accountId>`         |
| `reopenChannel` | BroadcastChannel | `sync:reopen:<accountId>`         |
| `textPeerLocks` | Web Locks        | `sync:textpeer:<clientId>:<slot>` |

Call `storageNames(prefix, accountId)` to obtain the names used by a given prefix and account.

## React layer

The `syncplane/react` subpath provides React hooks and context providers.
React 18 or later is an optional peer dependency.

```tsx
import { createReactSync } from 'syncplane/react';
import { TodoSchema } from './gen/example/todo/v1/todo_pb.js';

const entities = [ TodoSchema ] as const;
const sync = createReactSync(entities);

function App() {
	return (
		<sync.SyncProvider
			options={ {
				url: 'https://sync.example.com',
				workspaceId: 'ws-1',
				entities,
			} }
		>
			<TodoList />
		</sync.SyncProvider>
	);
}
```

### createReactSync

`createReactSync(entities)` returns a bundle of typed hooks and components:

```ts
interface ReactSync<E> {
	SyncProvider: typeof SyncProvider;
	WorkspaceProvider: typeof WorkspaceProvider;
	db: ReactEntityApis<E>;
	useSyncStatus: typeof useSyncStatus;
	useWorkspaceStatus: typeof useWorkspaceStatus;
	useCan: typeof useCan;
	useCanCreate: typeof useCanCreate;
	useHasPermission: typeof useHasPermission;
	useUndo: typeof useUndo;
}
```

### Per-entity hooks

The `sync.db.<entity>` namespace provides the following hooks:

```ts
interface ReactEntityApi<D> {
	useQuery(
		q: Query<MessageShape<D>>,
		opts?: UseQueryOptions,
	): WatchResult<MessageShape<D>>;
	useSuspenseQuery(
		q: Query<MessageShape<D>>,
		opts?: UseQueryOptions,
	): WatchResult<MessageShape<D>>;
	useQueryAll(
		q: Query<MessageShape<D>> | WorkspaceQuery<MessageShape<D>>,
		opts?: UseQueryAllOptions,
	): WatchResult<MessageShape<D>>;
	useGroupCount(
		q: GroupQuery<MessageShape<D>>,
		opts?: UseWorkspaceOptions,
	): GroupResult;
	useDistinct(
		q: GroupQuery<MessageShape<D>>,
		opts?: UseWorkspaceOptions,
	): DistinctResult;
	useGroupRows(
		q: GroupRowsQuery<MessageShape<D>>,
		opts?: UseWorkspaceOptions,
	): RowGroupResult<MessageShape<D>>;
	useRow(key: string, opts?: UseRowOptions): UseRowResultFor<D>;
	useSuspenseRow(key: string, opts?: UseRowOptions): UseRowResultFor<D>;
	useRows(keys: readonly string[], opts?: UseRowsOptions): UseRowsResultFor<D>;
	useSuspenseRows(
		keys: readonly string[],
		opts?: UseRowsOptions,
	): UseRowsResultFor<D>;
	useRowsAll(
		refs: readonly RowRef[],
		opts?: UseRowsAllOptions,
	): UseRowsResultFor<D>;
	useMutate(opts?: UseWorkspaceOptions): EntityMutate<D>;
	useText(key: string, field: string, opts?: UseTextOptions): UseTextResult;
}

interface UseRowOptions extends UseWatchOptions {
	hold?: boolean; // keep the row in scope while mounted
}

interface UseTextOptions extends UseWorkspaceOptions {
	hold?: boolean; // keep the row in scope while mounted; open once it has synced
}
```

Hook result types:

```ts
type WatchResult<T> = ReadonlyRowSet<T> & WatchState;

interface WatchState {
	readonly loading: boolean;
	readonly hydrating: boolean;
	readonly error: Error | undefined;
}

type GroupResult = readonly Group[] & Omit<WatchState, 'hydrating'>;
type DistinctResult =
	& readonly (Scalar | undefined)[]
	& Omit<WatchState, 'hydrating'>;
type RowGroupResult<T> = readonly RowGroup<T>[] & Omit<WatchState, 'hydrating'>;
```

### The hooks

- **`useQuery(query, opts?)`** — the query is compiled and structurally hashed on
  every render: a structurally equal query (new
  lambdas included) reuses the subscription, a changed one resubscribes. The
  result is the same `coverage`-carrying array `client.query` returns, and it
  keeps its reference until the rows change, so a memoized child does
  not re-render on unrelated commits.

  It also carries `loading`, `hydrating` and `error` — non-enumerable, like
  `coverage`, so `toEqual([])` and `JSON.stringify` do not see them:

  ```tsx
  const todos = db.todo.useQuery({ where: (t) => t.completed.eq(false) });
  if (todos.loading || todos.hydrating) return <Spinner />; // vs. genuinely empty
  if (todos.error) return <Failed error={ todos.error } />;
  return <ul>{ todos.map((t) => <Todo key={ t.key } row={ t } />) }</ul>;
  ```

  `loading` is true until the first callback — the only way to tell "not read
  yet" from "read, and empty", since both are `[]`. `hydrating` is
  the same question one level out: a cold client's first snapshot lands entity
  by entity, so a result can be read, empty, and still on its way. Guard an
  empty state with both. Setting `error` means the subscription could not be
  established at all (the store never came up): no rows will ever
  arrive; rows already delivered stay. `{ include }` hydrates references and
  is part of the subscription identity.

- **`useQueryAll(query, opts?)`** — the same read across workspaces, as one
  subscription: every workspace the client holds,
  or the set `opts.workspaces` names. Each row carries the `workspaceId` it came
  from, which is what groups the result and routes a write back through
  `client.ws(row.workspaceId)`. Use it when the workspaces are a list the
  app learns at runtime. It takes no `workspaceId` and has no suspense
  flavor. See [Hold several workspaces on one client](../how-to/multiple-workspaces.md).

  ```tsx
  const everywhere = db.todo.useQueryAll({
  	where: (t) => t.completed.eq(false),
  });
  const byWorkspace = Map.groupBy(everywhere, (row) => row.workspaceId);
  ```

  `opts.workspaces` narrows the read to workspaces you name: only those
  workspaces are read, and `total` counts only them. It joins the subscription
  identity as a **set**, so a fresh array literal per render is fine and only a
  changed set resubscribes. Cross-workspace reads use
  [secondary indexes](proto-options.md#secondary-indexes) for any number of
  workspaces, with or without the `workspaces` option. Without the option, the
  read covers every workspace holding rows of the entity in the local store.

  ```tsx
  const followed = db.todo.useQueryAll(
  	{ where: (t) => t.completed.eq(false) },
  	{
  		workspaces: followedIds,
  	},
  );
  ```

  Passing `byWorkspace` in place of `where` gives each workspace its own
  predicate under the same single subscription — the way to express "assigned to
  me" when the id that means "me" differs per workspace. It requires
  `opts.workspaces`; see
  [Hold several workspaces on one client](../how-to/multiple-workspaces.md).

- **`useRow(key, opts?)`** — `{ row, loading, hydrating, error, patch, update,
  del }`, where writers use per-field last-writer-wins like every
  other mutate call. Pass `{ cas: true }` to guard one on the row's current
  `version` (see [compare-and-swap](#compare-and-swap)). They
  reject with a clear error while the row is undefined.
  `loading`/`hydrating` separate the three reasons the row can be undefined:
  not read yet, rows still arriving, or all rows here and
  this key is not one.

  All `useRow` hooks on the same workspace, entity and `include` share one
  subscription that reads exactly the keys those hooks hold. A list of row
  hooks costs one watch and one point read per row; nothing scales with
  entity size. A commit re-renders only hooks whose own row moved.

  Pass `{ hold: true }` to keep the row in scope while the component is
  mounted, for a row the declared subscriptions do not cover (see
  [holds](#holds-one-row-in-scope-on-demand)). The hold is released on unmount
  and when the key changes.

- **`useRows(keys, opts?)`** — the same subscription, read as a set: one result
  slot per key, in the order given, `undefined` where a key names no row.
  `loading` is true until every key is answered. Use it when the
  keys come from data.

  ```tsx
  const selected = db.todo.useRows(selectedIds);
  const titles = selected.filter((row) => row !== undefined).map((row) =>
  	row.value.title
  );
  ```

  Growing `keys` reads only the added keys; held rows are not
  re-read, and the returned array keeps its identity while nothing in it moves.

- **`useRowsAll(refs, opts?)`** — `useRows` over rows named by workspace **and**
  key, so one call site can hold rows from several workspaces at once. This is
  the cross-workspace read `useQueryAll` cannot express: a set assembled outside
  the entity — a personal task list, a pinned set, ids stored elsewhere —
  that no predicate describes. The same key in two workspaces is two refs and
  two slots.

  ```tsx
  // `pinned` is [{ workspaceId, key }, ...], held wherever the app keeps it.
  const tasks = db.todo.useRowsAll(pinned);
  const byWorkspace = Map.groupBy(
  	tasks.filter((row) => row !== undefined),
  	(row) => row.workspaceId,
  );
  ```

  It ignores the enclosing `<WorkspaceProvider>`, takes no `workspaceId` and has
  no suspense flavor, matching `useQueryAll`. Each row names the workspace it
  came from, which is what routes a write back through
  `client.ws(row.workspaceId)`. Every `useRowsAll` on one entity and `include`
  shares a single cross-workspace subscription; that one is separate from the
  per-workspace subscription `useRow`/`useRows` share, so a row read through both
  is read twice.

- **`useSuspenseQuery(query, opts?)` / `useSuspenseRow(key, opts?)` /
  `useSuspenseRows(keys, opts?)`** — the same
  hooks, waiting for the first callback instead of reporting `loading`. A
  subscription that fails throws to the nearest error boundary.

- **`useMutate(opts?)`** — the stable `{ put, patch, update, del }` facade.
  Mutations are optimistic, so these promises resolve on **local** commit; a
  server rejection arrives later, on the hook below.

- **`useOnMutationRejected(handler, opts?)`** — runs `handler` for every mutation
  that will never be applied: a server rejection, a poison drop, a purge, a
  format upgrade. This is what a "your change could not be saved" toast hangs
  off:

  ```tsx
  useOnMutationRejected((info) => {
  	const what = info.targets.map((t) => `${t.entity} ${t.key}`).join(', ');
  	toast(`Could not save ${what}: ${info.errorMessage}`);
  });
  ```

  `info.targets` names the rows the mutation would have written (one for a CRUD
  mutation, one per member for a batch), and `info.mutator` names the custom
  mutator when it was one. The handler is held in a ref, so an inline arrow does
  not resubscribe. Only the hook's resolved workspace is delivered by default;
  pass `{ allWorkspaces: true }` for an app-wide surface. A rejection whose push
  response was lost arrives a cycle late — see
  [Handle failures](../how-to/handle-failures.md).

- **`useText(key, field, opts?)`** — see
  [Use collaborative text](../how-to/use-collaborative-text.md). Pass
  `{ hold: true }` to keep the row in scope while mounted; the handle opens
  once the row has synced, so a document outside the declared subscriptions
  can be edited without a scope change.

- **`useCan(schema, key, operation, opts?)`** — `client.can` as a hook. `opts`
  takes `fields`, `detail` and `workspaceId`. Re-asks when an access input of
  the row changes: whether the row is held, its access parent, or a field that
  a check of the access rules or field guards reads (a `callerIs` field, a field
  a `when` condition reads, and the same for a `within` entry on the entity
  itself), and when `onAccessChange` reports the workspace. For a `'delete'`,
  it also re-asks when a row is written of an entity whose rows can name
  the row as their access parent or their subject. Any other field change
  does not re-ask. The first render answers at once: `'yes'` or `'no'`
  when the summary the client read last decides it without a row, else the answer
  the same question got last in this client, else `'unknown'`. An answer from
  memory can be out of date until the hook's first read of the store finishes.
  With `{ detail: true }`, the hook returns a `CanDetail`, whose reason is
  `'loading'` before that first read. See
  [Predicting access](#predicting-access).
  **`useCanCreate(schema, row, opts?)`** — `client.canCreate` as a hook; re-asks
  when `row` changes by value. **`useHasPermission(schema, key,
  permission, opts?)`** — `client.hasPermission` as a hook; re-asks as
  `useCan` does, except that the fields the checks read are not inputs. Both take
  `detail` and `workspaceId`, and start as `useCan` does.

- **`useSyncStatus()`** — the live account-wide status object.
  **`useWorkspaceStatus(opts?)`** — one workspace's slice of it (its pending
  count, scope, access verdict and first-snapshot progress), which a busy
  neighbour workspace does not re-render.

  `hydration` is set only while a cold client is still downloading its first
  snapshot — `{ pending, total, done }`, so a one-off progress bar is
  `done / total` and disappears on its own:

  ```tsx
  const { hydration } = useWorkspaceStatus();
  if (hydration) {
  	return <Loading done={ hydration.done } total={ hydration.total } />;
  }
  ```

- **`useUndo(opts?)`** — `{ canUndo, canRedo, undo, redo }` over the resolved
  scope's undo stacks: the nearest `<UndoScopeProvider>`'s if there is one, else
  the workspace's. Stacks never span workspaces, because the mutation ids they
  reference do not.

All of them are built on `useSyncExternalStore` with an SSR snapshot, and
nothing hands out a mutable row: results are readonly, and snapshots are stable
and tearing-free.

### Workspace resolution

Hooks resolve their target workspace using the following priority order:

1. The `{ workspaceId }` option passed directly to the hook.
2. The enclosing `<WorkspaceProvider workspaceId="...">` context.
3. The client's `defaultWorkspace`.

If no workspace is resolved, the hook throws an error.

### Top-level hooks and components

```ts
useSyncClient(): SyncClient
useWorkspaceScope(workspaceId?: string): WorkspaceClient
useWorkspaceId(): string | undefined
useSyncStatus(): SyncStatus
useWorkspaceStatus(opts?: UseWorkspaceOptions): WorkspaceStatus
useUndo(opts?: UseWorkspaceOptions): { canUndo: boolean; canRedo: boolean; undo: () => Promise<boolean>; redo: () => Promise<boolean> }
useUndoScope(opts?: UndoScopeOptions): UndoScope
useNearestUndoScope(): UndoScope | undefined
useOnMutationRejected(handler: (info: RejectionInfo) => void, opts?: UseOnMutationRejectedOptions): void

<SyncProvider options={SyncClientOptions} />
<SyncProvider client={SyncClient} />
<WorkspaceProvider workspaceId={string} />
<UndoScopeProvider maxDepth?={number} />
```

`<UndoScopeProvider>` opens a [nested undo scope](#nested-undo-scopes) for its subtree.
`<SyncProvider>` accepts either `options` (constructing and managing a `SyncClient`) or an existing `client` instance.
Passing both or neither throws an error.

### Server components and SSR

The `syncplane/react` entry carries a `'use client'` directive, so a React
Server Components build (the Next.js App Router, for one) can import it directly
without an app-side wrapper. The hooks are still client-side: on the server they
render their empty snapshot, since the data lives in the browser's store.

## Other top-level exports

- `newId(): string`: Generates a time-ordered string identifier.
- Fractional indexing: `generateKeyBetween(a, b)`, `generateNKeysBetween(a, b, n)`, `BASE_62_DIGITS`. A `null` bound is open. Both throw on a bound that is not an order key or when `a >= b`. The Go package `engine/fractional` makes the same keys; see [Order keys](server.md#order-keys).
- [Custom mergers](proto-options.md#custom-mergers-for-opaque-fields): `MergerSpec`, `MergeFn`.
- [Naming rules](naming.md): `validateEntityName`, `validateKeyField`, `validateRowKey`, `MAX_ENTITY_NAME_LENGTH`, `MAX_ROW_KEY_BYTES`.
- [Storage names](#storage-names): `storageNames`, `DEFAULT_STORAGE_PREFIX`, `StorageNames`.
- Subscription builder: `subscription`, `SubscriptionSpec`.
- [Observability](telemetry.md#the-typescript-client): `SyncEvent`, `PushEvent`, `PullEvent`, `BootstrapEvent`, `RebaseEvent`, `PokeEvent`, `StreamEvent`, `LeadershipEvent`, `ResequenceEvent`, `QueryEvent`, `SyncEventHandler`, `SyncEventOutcome`, `QueryDeclineReason`.
- [Rooms](#rooms-presence-and-broadcast): `jsonCodec`, `RoomSendError`, `Room`, `Peer`, `RoomCodec`, `RoomError`, `RoomMessage`, `RoomOptions`, `RoomStatus`.
- Error codes: `CodeConflict`, `CodeInvalidArgument`, `CodePermissionDenied`, `CodePoisonDropped`, `CodeRoomDropped`, `CodeRoomOffline`, `CodeRoomClosed`.

## Rooms — presence and broadcast

Rooms provide ephemeral real-time messaging and presence tracking.
See [Rooms](../explanation/rooms.md) and [Add presence to an app](../how-to/add-presence.md).

```ts
ws.room<S, M>(name, opts?): Room<S, M>
ws.room<S, M>(schema, key, name, opts?): Room<S, M>
```

Anchoring a room to a schema and key inherits the read authorization of that row.

### Room interface

The `Room<S, M>` interface provides the following members:

| Member                                                     | Description                                                                                             |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `name: string`                                             | The room name.                                                                                          |
| `sessionId: string`                                        | Unique identifier for the local session.                                                                |
| `setPresence(state: S, opts?): void`                       | Updates local session state. Re-transmits when stream reconvenes.                                       |
| `send(message: M): Promise<void>`                          | Broadcasts a message to active peers. Rejects with `RoomSendError` if closed or dropped.                |
| `peers(): readonly Peer<S>[]`                              | Returns the active peer roster including the local session, sorted by `sessionId`.                      |
| `subscribe(cb: () => void): () => void`                    | Subscribes to roster changes. Returns an unsubscribe function.                                          |
| `onMessage(cb: (msg: RoomMessage<M>) => void): () => void` | Subscribes to broadcast messages. Returns an unsubscribe function.                                      |
| `onError(cb: (err: RoomError) => void): () => void`        | Subscribes to room errors and disconnections. Returns an unsubscribe function.                          |
| `status(): RoomStatus`                                     | Returns `'joining'`, `'open'`, `'waiting'` (no grant in the workspace yet), `'dropped'`, or `'closed'`. |
| `close(): void`                                            | Closes the room and withdraws local presence. Idempotent.                                               |

### Peer interface

The `Peer<S>` interface represents an active participant:

```ts
interface Peer<S> {
	readonly sessionId: string;
	readonly subject: string;
	readonly state: S | undefined;
	readonly seq?: bigint;
	readonly self: boolean;
}
```

### Room options

The `RoomOptions` interface accepts the following parameters:

| Option       | Type              | Default       | Description                                                |
| ------------ | ----------------- | ------------- | ---------------------------------------------------------- |
| `codec`      | `RoomCodec<S, M>` | `jsonCodec()` | Serializer for state and messages.                         |
| `throttleMs` | `number`          | `60`          | Minimum interval in milliseconds between presence updates. |

### React hooks for rooms

```ts
const { peers, room, loading, error } = useRoom<S, M>(name, opts?);
const peers = usePresence<S>(name, opts?);
```

Both hooks accept unanchored (`name, opts?`) or row-anchored (`schema, key, name, opts?`) signatures.
Rooms automatically close when the calling component unmounts.

### Multi-tab behavior

One watch stream runs per account, elected using Web Locks.
Publishing occurs directly from each tab via unary RPCs.
See [Multi-tab behaviour](../explanation/multi-tab.md).

## Test kit

The `syncplane/testing` package provides in-memory testing utilities.
See [Test your app without a server](../how-to/test-your-app.md).

```ts
import { createTestClient, createTestServer } from 'syncplane/testing';

const server = createTestServer();
const client = await createTestClient({ server, entities: [ TodoSchema ] });
```

Exported functions and types:

- `createTestClient(opts: TestClientOptions): Promise<TestClient>`: Returns an instance configured with an in-memory server.
- `createTestServer(opts?: TestServerOptions): TestServer`: Creates a shared in-memory sync server.
- `TestClient`: Implements `client`, `db`, `server`, `net` (`offline()`, `online()`, `isOffline`), `clientId`, `rejections`, `sync()`, and `close()`.
- `TestServer`: Implements `transport()`, `reject(match?, code?, message?)`, `holdPushes()`, `gc(workspace?)`, `revoke(id)`, `withholdAccess(id)`, `restore(id)`, `mutator(name, m)`, `setCallerAccess(access, { workspace? })`, and data inspection helpers (`put`, `del`, `get`, `rows`). `setCallerAccess` takes a `TestCallerAccess`: the caller's `subjects`, the `roles` it holds, and the `permissions` it holds outside a role. A role is held at the workspace root, or `at` a row (`[ ProjectSchema, 'p1' ]`), and grants the permissions of its declaration and of its `includes`. `setCallerAccess` throws for a role that no registered entity declares, and for an `at` row that the role's grant rows cannot point at. A permission is held at `root: true` or at the `anchors` rows. The call sets the access summary that every later bootstrap and pull carries, which `can` and `useCan` answer from. The test server does not check writes against it. The `maxAccessDepth` option of `new TestServer(...)` sets the `MaxAccessDepth` that each summary carries (default `32`).
- `TestServerOptions.serve`: `'all'` (default) sends every row. `'readable'` sends only the rows that a read rule lets the caller read, as the real server does. With it, a client does not hold the rows the caller cannot read, so `can` answers `'unknown'` for them.
- `Rejection`: Error class thrown inside test mutators to simulate server rejections.
- `DEFAULT_WORKSPACE`: Constant string `'main'`.

### Unsupported features

The testing kit does not simulate the following features:

- Partial-sync predicates, windows, and follows (every row syncs, or every readable row with `serve: 'readable'`).
- Write checks of the access model (use `server.reject` to refuse a write).
- Server queries (`queryServer` and related functions fail).
- Presence rooms.
- Multi-tab behavior (Node.js does not provide a shared lock manager).

To test these features against the engine, see [Testing](../how-to/testing.md).

## Quick mental model

- **Local-first**: Local reads and writes interact with local storage directly while background processes manage remote synchronization.
- **Last-writer-wins (LWW)**: `patch` operations merge field masks where server arrival sequence resolves conflicts. `put` operations replace rows entirely.
- **Reactivity**: Watchers and query subscriptions evaluate and re-render against local storage state. `queryServer` executes against server state without caching or overlay merging.
