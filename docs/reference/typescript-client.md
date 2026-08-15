# `syncplane` — JS/TS API Reference

A local-first sync client. Mutations apply **optimistically** to a local store and
queue locally (works fully offline); a background coordinator pushes the queue,
pulls server changes, and rebases. Reads are local and synchronous-after-await.

- **ESM-only.** `"type": "module"`.
- Package entry `.` (core) and `./react` (React hooks layer; `react` is an optional peer dep `>=18`).
- Generated proto descriptors are re-exported under `./gen/*`.
- Wire contract is protobuf. Entities are either compiled from `.proto` (via `buf`)
  or declared in TS with the `schema` builder (below). Either way you hand the
  client a tuple of `DescMessage` descriptors.

Two surfaces address the same engine:

1. **Schema-first** — methods on the client take a `DescMessage` schema as the first arg
   (`client.get(Todo, key)`). The escape hatch when two entities share a simple name.
2. **`client.db.<entity>`** — a per-entity typed facade derived from the message
   simple-name (`client.db.todo.get(key)`). Preferred. Throws at build time if two
   entities map to the same key.

Key generic convention throughout: `D extends DescMessage`, `MessageShape<D>` is the
decoded row object, `MessageInitShape<D>` is the partial init form accepted by writes.

---

## 1. Construction & lifecycle

```ts
import { SyncClient } from 'syncplane';
import { TodoSchema } from './gen/example/todo/v1/todo_pb.js';

const client = new SyncClient({
	url: 'https://sync.example.com', // or pass a prebuilt `transport`
	workspaceId: 'ws-123',
	entities: [ TodoSchema ], // `const`-inferred tuple → types `client.db`
});

await client.start(); // bootstrap (first run) + start poke/poll loops
// ... use client ...
await client.close(); // stop sync, drop watchers, close KV. One-shot; not reopenable.
```

### `new SyncClient(opts: SyncClientOptions<E>)`

`SyncClientOptions<E extends readonly DescMessage[]>`:

| Option                    | Type                                           | Notes                                                                                                                                                                                                                                               |
| ------------------------- | ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `entities`                | `E` (`readonly DescMessage[]`)                 | **Required.** All syncable entity descriptors. Pass as a `const` tuple so `db` types per entity.                                                                                                                                                    |
| `workspaceId`             | `string`                                       | Shorthand for `workspaces: [id], defaultWorkspace: id`. Give this **or** `workspaces`.                                                                                                                                                              |
| `workspaces`              | `string[]`                                     | Workspaces to sync. Add and remove more at runtime.                                                                                                                                                                                                 |
| `defaultWorkspace`        | `string`                                       | What the client-level API (`client.mutate`, `client.db`, `client.get/scan/query/watch`, `transact`, undo) binds to. Defaults to the only configured workspace; **required** past one.                                                               |
| `accountId`               | `string`                                       | Storage scope — one IndexedDB database, KV mutex, change channel and leadership lock per account. Defaults to the single workspace's id; **required** when starting with more than one.                                                             |
| `storagePrefix`           | `string`                                       | Stem of every name the client claims on the origin (database, Web Locks, channels). Default `'sync'`; letters, digits, `_` and `-`, starting with a letter or digit. See [Storage names](#storage-names).                                           |
| `url`                     | `string`                                       | Server base URL. Either `url` **or** `transport` required.                                                                                                                                                                                          |
| `transport`               | `Transport` (`@connectrpc/connect`)            | Prebuilt Connect transport (tests / Node).                                                                                                                                                                                                          |
| `wireFormat`              | `'binary' \| 'json'`                           | Encoding of the transport built from `url`. Default `'binary'`; `'json'` trades size and decode cost for traffic you can read in the network inspector. Ignored when `transport` is given.                                                          |
| `kvStore`                 | `KVStore`                                      | Defaults to IndexedDB in browsers, in-memory elsewhere.                                                                                                                                                                                             |
| `mutators`                | `Record<string, MutatorMirror<DescMessage>>`   | Best-effort local mirrors of server-side custom mutators.                                                                                                                                                                                           |
| `mergers`                 | `MergerSpec[]`                                 | Client mirrors of server merge hooks for opaque fields; divergence self-heals on next rebase.                                                                                                                                                       |
| `onMutationRejected`      | `(info: RejectionInfo) => void`                | Dead-letter callback (rejection or poison drop). A rejection whose push response was lost arrives a cycle late, not never.                                                                                                                          |
| `onStatusChange`          | `(status: SyncStatus) => void`                 | Same as `onStatusChange()` method.                                                                                                                                                                                                                  |
| `onSyncEvent`             | `(event: SyncEvent) => void`                   | Observability seam: push/pull/bootstrap/rebase/poke/stream/leadership events, plus a `query` event per local read saying whether an index or a scan answered it. No OpenTelemetry dependency — see [Telemetry](telemetry.md#the-typescript-client). |
| `pollIntervalMs`          | `number`                                       | Fallback poll; `0` disables. Default 60s.                                                                                                                                                                                                           |
| `pokeEnabled`             | `boolean`                                      | Maintain poke stream. Default `true`.                                                                                                                                                                                                               |
| `streamEnabled`           | `boolean`                                      | Hold the live change stream open. Default `true`; `false` leaves the poll and poke paths to carry changes.                                                                                                                                          |
| `pushDebounceMs`          | `number`                                       | How long a local write waits before it is pushed, restarted by each further write, so a burst ships as one compacted push. Default 50ms; `0` pushes on every write.                                                                                 |
| `pushMaxDelayMs`          | `number`                                       | Ceiling on that wait, from the first write of a burst, so continuous writing still pushes regularly. Default 250ms; `0` removes the ceiling.                                                                                                        |
| `pullLimit`               | `number`                                       | Max row changes per pull page; `0` = server default.                                                                                                                                                                                                |
| `poisonThreshold`         | `number`                                       | Identical push failures of the head mutation (RPC `invalid_argument`, or a batch refused as a whole) before dropping it to dead-letter. Default 5; `<=0` disables.                                                                                  |
| `maxMutationsPerPush`     | `number`                                       | Max batch per push RPC. Default 256.                                                                                                                                                                                                                |
| `maxWorkspacesPerRequest` | `number`                                       | Most workspaces one request may name (push batches, Subscribe scopes). Default 64. A client holding more splits its pushes; the extra workspaces sync by poll rather than stream.                                                                   |
| `subscriptions`           | `SubscriptionSpec[]`                           | Partial sync. Empty/absent = full sync.                                                                                                                                                                                                             |
| `bootstrapPriority`       | `readonly DescMessage[]`                       | Entities the first snapshot delivers first — what the first screen renders. A reordering, never a filter; only cold starts feel it.                                                                                                                 |
| `schemaRevision`          | `string`                                       | Opaque token matched against server's; drives skew handshake.                                                                                                                                                                                       |
| `onSchemaSkew`            | `(info: {client, server}) => SchemaSkewAction` | `'ignore' \| 'warn' \| 'block' \| 'resync'`. Default `'warn'` when `schemaRevision` set.                                                                                                                                                            |
| `undo`                    | `UndoOptions`                                  | Enables in-memory undo/redo.                                                                                                                                                                                                                        |
| `debugVerifyTouches`      | `boolean`                                      | Cross-check every maintained watch result against a full scan and throw on a difference. Development aid; doubles the read cost of every commit. Default from `SYNC_DEBUG_VERIFY_TOUCHES`.                                                          |
| `debugVerifyIndexReads`   | `boolean`                                      | The same check for index-served queries against the equivalent scan, totals included. Same default.                                                                                                                                                 |

Only the entities in `entities` are stored: a full-sync client is sent whatever
the server has, and rows it has no schema for are dropped rather than kept.
Adding an entity to a later build therefore re-bootstraps the workspace once
([why](../explanation/convergence-model.md#a-client-holds-only-the-entities-it-declares)).

Callbacks are invoked in isolation: a throw is logged to `console.error` and
dropped, never failing the write or sync cycle that reported the event, and
never skipping the callbacks behind it. A throwing `onSchemaSkew` yields
`'warn'`.

### Lifecycle / control methods

```ts
client.defaultWorkspace: string | undefined  // readonly; undefined when there is no default
client.workspaceId: string                   // readonly; the default workspace, throwing when there is none

await client.start(): Promise<void>          // idempotent + retryable; begins background sync
await client.close(): Promise<void>          // idempotent; teardown. Not reopenable.
await client.syncNow(): Promise<void>        // force one push+pull cycle (mostly tests). Needs start() first.
await client.clientId(): Promise<string>     // stable per-device id (persisted in KV)
await client.pendingCount(): Promise<number> // locally queued, not-yet-acked mutations

client.status(): SyncStatus                          // snapshot (works before start())
client.onStatusChange(cb): () => void                // subscribe; returns unsubscribe
client.onMutationRejected(cb): () => void            // multi-listener sibling of the option
client.onSyncEvent(cb): () => void                   // multi-listener sibling of the option
client.setSubscriptions(specs: SubscriptionSpec[]): void  // swap partial-sync scope at runtime
```

### Workspaces

The client-level read, write, `transact` and undo APIs target
`defaultWorkspace`; with no default they throw and everything goes through
`client.ws(id)`. See
[Hold several workspaces on one client](../how-to/multiple-workspaces.md).

```ts
client.ws(workspaceId): WorkspaceClient<E>   // throws for a workspace not held
client.addWorkspace(workspaceId, specs?): WorkspaceClient<E>  // bootstraps on the next cycle
await client.removeWorkspace(workspaceId, opts?): Promise<RemoveWorkspaceResult>

interface RemoveWorkspaceOptions {
	purge?: boolean; // also delete the local rows, overlay, cursor and queue
}
interface RemoveWorkspaceResult {
	removed: boolean; // false when the client did not hold it — the call is idempotent
	purged: boolean;
	discarded: DiscardedMutation[]; // queued mutations the purge dropped
}
```

A purge discards whatever was still queued for the workspace. Those mutations
can never be acked, so they come back in `discarded` — read inside the
transaction that deleted them, so the list is exactly what was lost — and each
also reaches `onMutationRejected` with code `workspace_purged`. Removing the
default workspace leaves the client without one.

A `WorkspaceClient` carries the same surface bound to one workspace: the reads,
`mutate`, `db`, `transact`, `text`, `undoScope`, `pendingCount`,
`setSubscriptions` and a `status()` returning that workspace's
`WorkspaceStatus`. Cross-workspace reads (`scanAll`, `queryAll`,
`watchQueryAll`) stay on the client and tag each row with its `workspaceId`.

---

## 2. Reads

All reads merge the optimistic overlay over confirmed server state. Both the
schema-first form and the `db` facade form are shown.

```ts
// schema-first
await client.get(Todo, key):    Promise<MessageShape<D> | undefined>
await client.getRow(Todo, key): Promise<Row<MessageShape<D>> | undefined>
await client.scan(Todo):        Promise<RowSet<MessageShape<D>>>
await client.query(Todo, q, { include? }): Promise<RowSet<MessageShape<D>>>
await client.queryServer(Todo, q):         Promise<RowSet<MessageShape<D>>>  // server push-down; NOT merged with overlay

// db facade (identical semantics)
await client.db.todo.get(key)
await client.db.todo.getRow(key)
await client.db.todo.scan()
await client.db.todo.query(q, { include? })
await client.db.todo.queryServer(q)

// across workspaces; each row names the one it came from
await client.scanAll(Todo):                              Promise<RowSet<MessageShape<D>>>
await client.queryAll(Todo, q, { include?, workspaces? }): Promise<RowSet<MessageShape<D>>>
```

`queryAll` spans every workspace the client holds unless `workspaces` names a
set, in which case only those prefixes are scanned and `total` counts only them.
An empty array reads nothing — it is not shorthand for "all" — and a workspace
the client does not hold contributes no rows. A set naming exactly one workspace
is read on that workspace's prefix and its indexes, which is what a read spanning
workspaces cannot use; cursors stay unavailable at every set size (see §4).

In place of `where`, a query may carry `byWorkspace: (workspaceId) => where |
undefined`, giving each workspace its own predicate under one merged result —
for a condition naming something workspace-scoped, a row id being the usual case.
Only the predicate varies (`orderBy`, `limit`, `offset` and `total` describe the
merged result and stay shared), returning `undefined` leaves that workspace out,
and it requires `workspaces`. `orderBy` may rank by `relevance()`, in which case
every workspace's predicate needs its own `match` (see §4).

Every result array is a `RowSet`: an ordinary array carrying non-enumerable
extras — `coverage` (see §9), `total`, and the three paging fields below. All of
them stay out of `JSON.stringify`, `Object.keys` and structural equality.

```ts
type RowSet<T> = Row<T>[] & {
	readonly coverage: Coverage;
	readonly total: number; // undefined when the query set `total: false`
	readonly startCursor?: QueryCursor; // position of the first row; absent when empty
	readonly endCursor?: QueryCursor; // position of the last row
	readonly hasMore: boolean; // matches remain past the end `limit` counted from
};
```

`total` counts the matches before the page was cut, over what the read could see
— which is what `coverage` describes: a local read counts synced rows, and
`queryServer` counts only the page the server returned (a full page also carries
the `window` bound, which is how a truncated one is recognised). A query
carrying `after`/`before` counts the matches INSIDE those bounds, since a row
outside them is not a match of that query. An index that stops early on a `limit`
still walks the rest of its range to count — positions only, with no row reads,
unless a residual predicate is left over, in which case only the row can settle a
match and the rest of the range is read too.

That count is the one part of an indexed page whose cost grows with the match
set rather than the page, and `total: false` in the query turns it off: `total`
comes back undefined (typed `undefined` when the flag is a literal in the query)
while the rows, cursors and `hasMore` are unchanged. Pass it on every page whose
consumer does not size anything with the total — cursor pages after the first
are the usual case, since a scroller sized itself on page one.

`startCursor`/`endCursor` are opaque `QueryCursor` tokens: base64url strings, so
they survive `JSON.stringify` and drop into a URL. Pass `endCursor` back as
`after` for the next page and `startCursor` as `before` for the previous one; see
§4. A cross-workspace read (`queryAll`, `scanAll`) mints none — a position names
a place in one workspace's ordering.

### Grouped reads — counts and distinct values

A grouped read answers with the GROUPS of a match set instead of its rows: the
numbers behind a board column's badge, a facet sidebar, or the values a filter
dropdown offers.

```ts
await client.groupCount(Issue, q):  Promise<Group[]>
await client.distinct(Issue, q):    Promise<(Scalar | undefined)[]>
await client.groupCountServer(Issue, q): Promise<Group[]>   // server push-down
await client.distinctServer(Issue, q):   Promise<(Scalar | undefined)[]>

// db facade (identical semantics)
await client.db.issue.groupCount(q)
await client.db.issue.distinct(q)
await client.db.issue.groupCountServer(q)
await client.db.issue.distinctServer(q)

interface GroupQuery<M> {
	by: (i: QueryRef<M>) => GroupKeyRef; // the field to partition by, named without an operator
	where?: (i: QueryRef<M>) => Cond;
	limit?: number; // maximum groups, counted from the first in value order
}

interface Group {
	value: Scalar | undefined; // undefined = the group of rows holding no value there
	count: number; // 0 from `distinct`, which counts nothing
}
```

```ts
const byStatus = await client.db.issue.groupCount({
	by: (i) => i.status,
	where: (i) => i.projectId.eq(projectId),
});
// [ { value: 'done', count: 12 }, { value: 'open', count: 4 } ]

const assignees = await client.db.issue.distinct({ by: (i) => i.assignee.id });
```

`by` names a field with no operator on the end — a group has none of its own, and
claiming one would make yet another field name unreachable through the DSL. It
accepts any comparable leaf and a repeated scalar; a message, a map, bytes and a
repeated message throw, because nothing identifies one of their values.

What a grouped read promises:

- **One group per distinct value**, plus one for the rows the path resolves to
  nothing on — `value: undefined`, and never conflated with an empty string or a
  zero.
- **Ascending value order**, with NaN and then the no-value group last: the same
  order an ascending `orderBy` on that path puts rows in. `limit` cuts from the
  front of it.
- **A repeated `by` path groups per ELEMENT.** A row holding three labels joins
  three groups, so the counts do not sum to the number of matching rows. That is
  what a label facet wants; a row count is a different question. A row whose list
  is empty joins the no-value group, so every matching row is somewhere.
- **Only groups the rows produce.** A status no row currently holds is absent
  rather than zero — the set of possible values is your schema's (an enum, a
  status entity), and the read joins against it rather than inventing it.

A [declared index](proto-options.md#secondary-indexes) whose components are
`[eq-filter fields…, groupPath, …]` — the same index a board ordering by
`[status, rank]` already wants — turns the read into a skip-scan: it seeks from
one distinct value to the next instead of walking rows, so a limited read costs
O(groups) rather than O(entity). Without one it buckets a scan, which is always
correct and never faster. See
[explanation/grouped-reads.md](../explanation/grouped-reads.md).

`groupCountServer`/`distinctServer` push the same read to the server, for counts
over rows partial sync did not bring down. They are server snapshots under the
same rules as `queryServer`, and unavailable inside `transact`.

### Grouped reads — the first rows of each group

`groupRows` partitions the match set the same way and answers with each group's
head: a board column's preview, the top item of every project.

```ts
await client.groupRows(Issue, q):       Promise<RowGroup<Issue>[]>
await client.groupRowsServer(Issue, q): Promise<RowGroup<Issue>[]>  // server push-down

// db facade (identical semantics)
await client.db.issue.groupRows(q)
await client.db.issue.groupRowsServer(q)

interface GroupRowsQuery<M> {
	by: (i: QueryRef<M>) => GroupKeyRef; // as in GroupQuery
	where?: (i: QueryRef<M>) => Cond;
	orderBy?: (i: QueryRef<M>) => OrderKey[]; // what "first" means inside a group
	perGroup: number; // rows per group
	limit?: number; // maximum groups, counted from the first in value order
	total?: boolean; // default true; see below
}

interface RowGroup<T> {
	value: Scalar | undefined; // undefined = the group of rows holding no value there
	rows: Row<T>[]; // at most `perGroup`, in `orderBy` order
	total: number | undefined; // matching rows in the group; undefined under `total: false`
	hasMore: boolean; // whether the group holds rows past the ones returned
}
```

```ts
const columns = await client.db.issue.groupRows({
	by: (i) => i.status,
	orderBy: (i) => [ i.rank.asc() ],
	perGroup: 20,
	where: (i) => i.projectId.eq(projectId),
});
```

The groups obey `GroupQuery`'s rules exactly — value order, the no-value group,
one group per element of a repeated path, and only the groups the rows produce.
Inside a group, rows come back in `orderBy` order with ties broken by row key
ascending, as they do in a `query`; without an `orderBy` they come back in row-key
order.

`total: false` drops the count and the walk of every matching row it needs, where
the rows themselves cost `perGroup` of them. Pass it on a column that shows no
badge.

An index whose components past the grouping path ARE the requested ordering —
`[eq-filter fields…, groupPath, order keys…]` — makes the read cost
`groups × perGroup` rows. An ordering it stores in the other direction, or one it
does not hold at all, falls back to reading the whole match set and sorting each
group. `groupRowsServer` pushes the read to the server under `queryServer`'s
rules; the server clamps `limit × perGroup` to its own row maximum and reports
`hasMore`.

### `Row<T>`

```ts
interface Row<T = unknown> {
	key: string;
	value: T;
	workspaceId: string; // always set; routes a write back through client.ws(id)
	version: bigint; // server-confirmed version this row is based on; 0n if optimistic-only.
	// Pass as `expectedVersion` for compare-and-swap.
}
```

- `get` returns the decoded value only; `getRow` returns the `Row` (with `version`,
  needed for CAS). `query`/`scan` return a `RowSet`.
- `workspaceId` is what makes a cross-workspace result (`scanAll`, `queryAll`,
  `watchQueryAll`) attributable — every row names the partition it came from.
- `queryServer` queries the server directly for data outside the synced subset.
  Results are server snapshots: not merged with the overlay, not stored, never feed
  watchers. Works before `start()`; requires connectivity. Ordering matches the
  local evaluator. Unavailable inside `transact` (no transport of its own).
  A declared [secondary index](proto-options.md#secondary-indexes) serves it when
  one fits the predicate and ordering; without one the call costs a full entity
  scan on the server, memory-bounded to `offset + limit` — so prefer a selective
  predicate over deep pagination.

---

## 3. Writes (mutations)

Mutations apply optimistically and enqueue. Two facades:

```ts
// schema-first — on `client.mutate`
await client.mutate.put(Todo, value, opts?)         // opts: CasOptions
await client.mutate.patch(Todo, key, init, opts?)   // opts: PatchOptions
await client.mutate.update(Todo, key, recipe, opts?) // opts: UpdateOptions
await client.mutate.del(Todo, key, opts?)           // opts: CasOptions
await client.mutate.custom(name, argsSchema?, args?)  // server-side custom mutator
client.mutate.list(Todo, rowKey, field): ListHandle  // movable ordered list

// db facade — on `client.db.<entity>.mutate`
await client.db.todo.mutate.create(value, opts?): Promise<string>  // mints id, returns key
await client.db.todo.mutate.put(value, opts?)
await client.db.todo.mutate.patch(key, init, opts?)
await client.db.todo.mutate.update(key, recipe, opts?)
await client.db.todo.mutate.del(key, opts?)
```

- **`put`** replaces the whole row.
- **`patch`** merges by field mask (per-field last-writer-wins). `opts.paths`
  defaults to the keys present in `init`. It does not create rows: a `patch`
  whose row is absent or deleted changes nothing locally and is rejected as
  `conflict`. The client warns on the console, naming the entity, key and
  workspace, when a `patch` has no row to land on — most often a key belonging
  to a different entity than the one it was written through.
- **`update`** is read-modify-write: it reads the row, hands a draft to
  `recipe`, and PATCHes the fields the recipe changed. See below.
- **`create`** (facade only) is `put` with an auto-minted id. Requires a single
  string key field; mints a fresh time-ordered id when the key is omitted/empty and
  returns it. Use `put` for composite/non-string keys or explicit keys.
- **`custom`** invokes a server-registered mutator by name; not on the `db` facade,
  not batchable, and an undo barrier.

### Which write verb

| You have                | Use      |
| ----------------------- | -------- |
| A whole row to write    | `put`    |
| The fields that changed | `patch`  |
| A row to edit in place  | `update` |

`patch` takes the fields to change, **not a whole row**. Passing a fetched
message is a compile error, because the mask would then name every field and the
PATCH would behave as a PUT — writing back stale values for fields the caller
never touched:

```ts
const t = await client.db.todo.get(id);
t.title = 'x';
await client.db.todo.mutate.patch(id, t); // ✗ compile error
```

### Read-modify-write — `update`

```ts
await client.db.todo.mutate.update(id, (t) => {
	t.title = 'x';
	t.completed = true;
});
```

The recipe edits a private draft of the row; only the fields it actually changed
are written. That is what makes it safe to save an edit made against a row read
some time ago: a field someone else changed meanwhile is not in the mask, so it
is not written back stale — which a `put` of the same edited row would do.

- Nothing changed ⇒ nothing is written (no mutation is queued).
- A missing row throws. Partial sync means a key can exist on the server without
  being here, and a silent no-op would hide that.
- The recipe is synchronous, and changing a key field is refused.
- Inside `transact`, the draft comes from the pre-batch row, like every other
  read on the handle.

To copy named fields off another message, pass an explicit mask — the mask, not
the object, decides what is written, so any message is accepted there:

```ts
await client.db.todo.mutate.patch(id, otherRow, { paths: [ 'title' ] });
```

Every write validates its row key before it touches the local store, so a key the
server would refuse throws here instead — see [Naming rules](naming.md).

### Compare-and-swap

```ts
interface CasOptions {
	expectedVersion?: bigint;
} // 0n means "only if row absent"
interface PatchOptions extends CasOptions {
	paths?: string[];
}
type UpdateOptions = CasOptions; // the mask comes from the diff
```

When `expectedVersion` is set, the server applies only if the row's current version
matches, else rejects with code `"conflict"` (optimistic edit rolls back,
`onMutationRejected` fires). Read the base version from `Row.version`
(`getRow`/`query`/`scan`/`watch`).

### Movable ordered lists — `ListHandle`

Returned by `client.mutate.list(schema, rowKey, field)` for a keyed `repeated Message`
field declaring an `order_field`. Operations read the current sorted snapshot,
allocate fractional positions, and enqueue ordinary per-item PATCHes.

```ts
interface ListHandle {
	insertAt(index: number, itemInit: Record<string, unknown>): Promise<string>; // returns element id
	insertBefore(
		beforeId: string,
		itemInit: Record<string, unknown>,
	): Promise<string>;
	move(id: string, toIndex: number): Promise<void>;
	remove(id: string): Promise<void>;
}
```

### Transactions — `client.transact(fn)`

```ts
await client.transact(async (tx) => {
	await tx.db.todo.mutate.put({ id, title: 'a' });
	await tx.mutate.patch(Todo, otherKey, { done: true });
});
```

CRUD writes issued **through the `tx` handle** collect into ONE atomic
`BatchMutation` (one overlay update, one queue entry, one push, one savepoint, one
ack-or-rejection, one undo step). Notes:

- Reads on `tx` see pre-batch state; buffered writes are invisible until `fn` returns.
- If `fn` throws, the whole batch is discarded.
- Scoping is **lexical-by-handle, not temporal**: a `client.mutate.*` issued during
  one of `fn`'s awaits is NOT captured — it queues independently.
- `tx` exposes all reads (`get/getRow/scan/query/watch/watchQuery/resolveRef/
  resolveReverseRef`), `tx.mutate` (CRUD only — no `custom`), and `tx.db`.

`Transaction<E>` shape mirrors the read methods + `mutate: CrudMutateApi` + `db: EntityApis<E>`.

---

## 4. Queries — the DSL

A `Query<M>` is a plain object of lambdas built against a typed proxy reference:

```ts
interface Query<M> {
	where?: (w: QueryRef<M>) => Cond;
	orderBy?: (w: QueryRef<M>) => OrderKey[];
	limit?: number;
	offset?: number;
	after?: QueryCursor;
	before?: QueryCursor;
	total?: boolean; // default true; false skips the match count (see §2)
}
```

Example:

```ts
import { and, not, or } from 'syncplane';

const rows = await client.db.todo.query({
	where: (t) => and(t.done.eq(false), t.title.contains('milk')),
	orderBy: (t) => [ t.createdAt.desc() ],
	limit: 20,
});
```

### Paging — `after` / `before`

`after` and `before` are exclusive bounds on the ordering, taken from an earlier
result of the SAME query:

```ts
const page = await client.db.todo.query({
	orderBy: (t) => [ t.createdAt.desc() ],
	limit: 50,
});
const next = await client.db.todo.query({
	...same,
	limit: 50,
	after: page.endCursor,
});
const prev = await client.db.todo.query({
	...same,
	limit: 50,
	before: page.startCursor,
});
```

- `limit` counts forward from `after`, and BACKWARD from `before` when `before`
  is the only bound — so `before` alone returns the page preceding the cursor's.
  Both set reads a bounded range, paged forward. Rows come back in `orderBy`
  order whichever bounds are set.
- A cursor is tied to the `(entity, orderBy)` pair it was issued under. Replaying
  one against a different entity or ordering throws `CursorShapeError` rather
  than returning a wrong page, so changing `orderBy` means starting a fresh page.
- `query` and `queryServer` mint and accept the same cursors, so a page started
  against the server can continue locally and the other way round.
- `offset` is the way to jump to an arbitrary rank, which a cursor cannot name.
  Setting it alongside either cursor throws.
- Cursors are unavailable on `queryAll`/`watchQueryAll`: a position names a place
  in one workspace's ordering, and a cross-workspace result has no single one.
  This holds however few workspaces the read spans — a set of one still mints
  none, so a list shrinking to a single workspace cannot silently change what the
  read returns.

### Operator surface (by field type)

The proxy maps each field to operators by its value type. **Operator names are
reserved** — a proto field literally named `eq`/`has`/`isSet` is shadowed and
cannot be filtered or ordered on; registering such a schema logs a warning
naming the field. **Enums arrive as numbers**, so it is `t.status.eq(2)`, not
`t.status.eq(Status.DONE)`.

| Field type                                      | Operators                                                                                      |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `string` (`StringOps`)                          | `eq`, `neq`, `in`, `startsWith`, `endsWith`, `contains`, `isSet`, `isNotSet` + order           |
| `number`/`bigint` (`NumericOps`)                | `eq`, `neq`, `in`, `gt`, `gte`, `lt`, `lte`, `isSet`, `isNotSet` + order                       |
| `boolean` (`BoolOps`)                           | `eq`, `neq`, `isSet`, `isNotSet` + order                                                       |
| `Uint8Array` & singular message (`PresenceOps`) | `isSet`, `isNotSet`                                                                            |
| repeated scalar (`RepeatedScalarOps`)           | `has`, `hasAny`, `hasAll`, `isEmpty`                                                           |
| repeated message (`RepeatedMessageOps`)         | `any(cb)`, `isEmpty`; on a reference also `matches(cb)`                                        |
| singular message                                | drillable (`t.settings.color.eq(...)`) + `isSet`/`isNotSet`; on a reference also `matches(cb)` |
| oneof group (`OneofRef`)                        | one entry per case: the member's operators + `isSet`/`isNotSet`                                |
| map (`MapOps`)                                  | `at(key)` → the value's operators + `isSet`/`isNotSet`; `any(cb)`; `isEmpty`                   |
| the query root (`EntityQueryOps`)               | `match(query)`; `relevance()` in `orderBy`                                                     |

Order: every orderable field exposes `.asc()` / `.desc()` → `OrderKey`.

Boolean combinators (top-level exports): `and(...conds)`, `or(...conds)`, `not(cond)`.

#### Full-text match — `match` / `relevance`

Entity-level, on the query root: `match(query)` is true when every term of the
query occurs in some [declared search field](proto-options.md#full-text-search)
of the row, case- and accent-insensitively, with the final term matching as a
prefix when the query ends inside it. An empty query matches every row.
`relevance()` orders best match first and requires exactly one `match` in the
query's `where` — the rank is that condition's.

```ts
await client.db.article.query({
	where: (a) => and(a.match('milk cow'), a.state.eq('published')),
	orderBy: (a) => [ a.relevance() ],
	limit: 20,
});
```

Both compose like any other condition and order key: `watchQuery` keeps a
ranked result live, `queryServer` runs the identical semantics over everything
the server holds, and relevance cursors page across the two. Thrown at query
time: `match` on an entity that declares no search fields, `match` anywhere but
the query root (including inside `any()`), `relevance()` with no `match`, with
several, or alongside a `path`. See [Search data](../how-to/search.md) and
[the search model](../explanation/search.md).

A cross-workspace read ranks its merged result the same way. Under `byWorkspace`
(§2) the requirement applies to each workspace's predicate rather than to a
shared one — a workspace whose predicate carries no `match`, or more than one,
throws and is named — and a row ranks against the match its own workspace was
read under. Ranks stay comparable across the legs because a rank is computed
from the row alone.

#### Asking about one entry of a map

`at(key)` addresses one entry. What it returns is the operator surface of the
map's **value** type, so an entry behaves exactly like a field of that type:

```ts
await client.db.issue.query({ where: (i) => i.attrs.at('color').eq('red') });
await client.db.issue.query({
	where: (i) => i.custom.at('priority').kind.number.gt(3),
});
await client.db.issue.query({
	where: (i) => i.custom.at('team').kind.text.eq('platform'),
});
// multi-select: a repeated scalar wrapped in one variant
await client.db.issue.query({
	where: (i) => i.custom.at('labels').kind.tags.values.hasAny([ 'ui', 'bug' ]),
});
```

`at()` rather than property access, for two reasons. A key that happens to
collide with an operator name (`eq`, `in`, `has`, `isSet`, …) would be silently
swallowed by the proxy — a trap that depends on the DATA rather than on the
schema. And `Record<string, V>` is structurally permissive enough that plain
property access would type-check against surfaces it should not.

When the value is an app-defined oneof — the usual shape for user-defined
fields — a query names the **variant** it means, through the group exactly as
anywhere else. An entry holding a different variant reads as not found: the
condition is false and the ordering ranks it last. The consequence is that there
is no ordering by "whatever the value is" across variants; `orderBy` names one.
See [proto options](proto-options.md#maps-and-user-defined-fields).

`isSet()` on the ENTRY (`i.custom.at('priority').isSet()`) asks whether the key
is there at all. `isSet()` on the map itself is not offered — a map has implicit
presence, so it is always set; `isEmpty()` is what asks whether it holds
anything.

#### Asking about ANY entry of a map

`any(cb)` is the mirror of `at()`: where `at('priority')` names the field and
asks about its value, `any()` asks about the value and leaves the field open. The
element it scopes is one **entry** (`MapEntryRef`), whose two sides are `key` and
`value`:

```ts
// Which custom field holds 5 — a question no addressed path can ask.
await client.db.issue.query({
	where: (i) => i.custom.any((e) => e.value.kind.number.eq(5)),
});
// One entry satisfying BOTH halves.
await client.db.issue.query({
	where: (i) =>
		i.custom.any((e) =>
			and(e.key.startsWith('cf_'), e.value.kind.checked.eq(true))
		),
});
// Which rows have any of these fields set at all.
await client.db.issue.query({
	where: (i) => i.custom.any((e) => e.key.in([ 'priority', 'severity' ])),
});
```

The entry scope is explicit for the same reason it is on a message list: it is
what lets two conditions describe the **same** entry.
`any((e) => and(e.key.eq('priority'), e.value.kind.number.eq(5)))` is "the field
named priority holds 5"; `and(m.any(…key…), m.any(…value…))` is the weaker
"priority is set AND something holds 5".

Two consequences:

- **The key is a value here**, not a path segment, so a key containing a dot
  needs no escaping and `in`/`startsWith` over keys come for free. Its operator
  surface is `MapKeyOps` rather than a derived one: a generated map types its
  keys as `string` whatever the .proto declared, so the surface cannot be read
  off the row shape. The value that reaches the evaluator IS the declared type —
  on a `map<int64, V>` the entry key is a bigint, so `e.key.gte(5n)` is the
  spelling that matches.
- **There is no projection.** A match says some entry satisfied the condition,
  never which one. Read the field's name off the row's map after the query
  returns.

#### Asking about the elements of a message list

`any(cb)` scopes one element and evaluates a condition against it. The reference
handed to the callback is rooted at the element, so its paths are relative:

```ts
await client.db.issue.query({
	where: (i) => i.assignees.any((a) => a.id.eq('maya')),
});

await client.db.order.query({
	where: (o) => o.lineItems.any((li) => and(li.sku.eq('A-1'), li.qty.gt(3))),
});
```

The element is explicit because two conditions have to be able to describe the
**same** element. Written as separate terms —
`and(o.lineItems.any((li) => li.sku.eq('A-1')), o.lineItems.any((li) => li.qty.gt(3)))`
— they are a different, weaker question that two different line items satisfy.

This is also how a to-many reference is filtered: an element is the target's
stub, so only its key fields are addressable and
`i.assignees.any((a) => a.id.eq('maya'))` is the list counterpart of
`i.project.id.eq(...)`. Asking for anything else on a reference element throws,
because a stored stub holds nothing else.

`any()` nests — `w.members.any((m) => m.tags.has('urgent'))` — each level rooted
at its own element. An empty or unset list matches nothing, so `not(...any())`
reads as "no element matches".

#### Crossing a reference — `matches()`

`matches(cb)` on a forward reference field — to-one or repeated — is the
one-hop semi-join: true when at least one referenced target row **exists** and
satisfies the condition. The callback's reference is rooted at the **target
entity's row**, so every field of the target is addressable:

```ts
// issues assigned to anyone on this team
await client.db.issue.query({
	where: (i) => i.assignee.matches((a) => a.teamId.eq(teamId)),
});
// tasks blocked by an unresolved task (repeated ref: SOME target matches)
await client.db.task.query({
	where: (t) => t.blockedBy.matches((b) => b.done.eq(false)),
});
```

Distinct from `any()` on the same field: `any()` scopes a **stub**, which
carries only the target's key fields, while `matches()` reaches the live target
row. `i.blockedBy.any((b) => b.id.eq(x))` asks about the stored reference
itself and stays row-local; `i.blockedBy.matches((b) => b.done.eq(false))`
crosses into the target entity. Both stay available.

Semantics worth pinning:

- An unset to-one ref, an orphaned stub and a deleted target all contribute
  nothing, so the condition is **false** for them — and `not(matches(…))`
  therefore also admits rows with no reference at all.
- The membership is decided per row **within the row's workspace**; references
  never cross the partition.
- Evaluation is a two-phase rewrite: the inner condition runs as an ordinary
  query over the target entity, and the node collapses to a membership test on
  the stored ref stubs — the same shape the reverse-relation push-down
  produces, so the derived per-ref-field index serves it.
- Cost: one pre-pass per distinct `matches` — two conditions crossing into the
  same entity with the same inner condition share one. It is an ordinary query,
  so an index declared on the target entity covering the inner condition makes
  it proportional to the matches rather than to the entity; a query over every
  workspace at once reads the entity instead, since an index walk covers one
  workspace.
- A **local** query crossing a reference is only as good as the target rows the
  store holds; when partial sync bounds them the result reports the
  `'relation'` coverage bound (see §9), and `queryServer` accepts the same
  condition as the escape hatch.
- Under `watchQuery`, a commit to the target entity re-evaluates the result even
  though it named no row the watch holds. Declaring an index on the watched
  entity whose first component is the reference field
  (`{ name: 'by_issue', key: [{ path: 'issue' }] }`) keeps that proportional to
  the rows the commit moved; without one, and for a `matches` under `not`, it is
  proportional to the watched entity. See
  [what a live `matches` costs](../explanation/relational-queries.md#what-a-live-matches-costs).

Rejected with named errors, all additive later: `matches` on a message field
that is **not** a reference (reference-ness lives in a proto option the TS types
cannot see, so this throws at query time, not compile time), `matches` inside
`matches` (the semi-join is one hop), `matches` inside `any()`, `matches` in a
subscription predicate (scope the base entity and `follow` the reference
instead), and `matches` in a forward relation's include `$where` (no query
backs it there; a reverse relation's `$where` takes it, since that rides a real
query). There is no `matches` **through a reverse relation** — "projects with
an open issue" is phrased from the source side or waits — and no ordering by a
related field.

### Rejected and surprising cases

- Comparing or ordering by a leaf that can never satisfy it — bytes, a message,
  a whole map, a repeated field — **throws when the query is compiled**, naming
  the operator that does work instead (for a map, the `at(key)` form). It does not quietly match nothing (or, for an
  ordering, tie every pair and fall back to row-key order).
- A mismatch between the literal and the field (`t.count.eq('x')` via an untyped
  path) is a legitimate no-match and evaluates false. Only the leaf itself being
  incomparable is an error.
- `isSet` follows proto presence: an `optional`, message or reference field is
  set only when written, but an implicit-presence scalar, list or map is
  **always** set. Compare against the zero value for "has a non-zero value".
- A oneof is filtered through its **members** — `w.choice.choiceText.eq('x')`,
  and `w.choice.choiceText.isSet()` for "this case is active", true even when the
  member holds a zero value. The group itself is not a leaf: a path stopping at
  it throws. Writes go the other way round and name the group, never a member
  (see [proto options](proto-options.md#oneofs)).
- `NaN` is comparable to nothing, matching JS: `gt`/`gte`/`lt`/`lte` are all
  false with a `NaN` on either side, as `eq` already is. Ordering ranks values
  `real < NaN < missing` and the rank wins over the direction, so `NaN` sorts
  after every real value and before missing under both `asc` and `desc`.
  `±Infinity` is ordinary.
- A string with a lone surrogate is not valid UTF-8, so the server sees U+FFFD.
  It is the one case where a local verdict and a `queryServer` verdict can
  legitimately differ.

### Underlying AST types (exported)

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
// Opaque and branded: read one off a RowSet, never build one.
type QueryCursor = string & { readonly __queryCursor: unique symbol };
```

`QueryRef`, `CompiledQuery`, `Query`, `QueryCursor`, `Cond`, `OrderKey`, `Path`
and `Scalar` are exported as types, and `CursorShapeError` as a value. The
converters that put a query on the wire are internal — the client sends one for
you, and the AST above is what you build.

---

## 5. Watchers (reactivity)

A watcher holds its result and maintains it: a commit names the rows it changed,
and each is spliced into or out of the result in place. The callback receives the
full row list whenever that result moves.

```ts
type WatchCallback<T> = (rows: RowSet<T>, delta: WatchDelta<T>) => void;

interface WatchHandle {
	close(): void;
	setWindow(window: { offset?: number; limit?: number }): void;
}

const view = client.watch(Todo, (rows) => { ... });
const view = client.watchQuery(Todo, q, (rows) => { ... }, { include?, window? });

// db facade
const view = client.db.todo.watch((rows) => { ... });
const view = client.db.todo.watchQuery(q, (rows) => { ... }, { include?, window? });

view.setWindow({ offset: 1200, limit: 50 });
view.close();
```

- Returns a **`WatchHandle`**, synchronously. `close()` is idempotent.
- A callback fires only when the delivered rows change — same rows at the same
  versions, no callback. With a `limit`, a change past `offset + limit` cannot
  move the page and does not fire.
- Cost per commit is the rows the commit named, not the size of the entity. A
  commit reporting an entity without naming rows (a bootstrap swap, a wholesale
  overlay clear) rescans it.
- Callbacks must **not mutate** received rows (shared between same-entity watchers).
- Cross-tab: a commit in another tab re-fires this tab's watchers (via BroadcastChannel).

#### Watching groups — `watchGroupCount` / `watchDistinct`

```ts
const view = client.watchGroupCount(Issue, q, (groups) => { ... }, { onError? });
const view = client.watchDistinct(Issue, q, (values) => { ... }, { onError? });

// db facade
const view = client.db.issue.watchGroupCount(q, (groups) => { ... });
const view = client.db.issue.watchDistinct(q, (values) => { ... });
```

- Same `GroupQuery` and the same answer as `groupCount`/`distinct` (§2), emitted
  after every commit touching the entity and suppressed when the answer is
  unchanged — so a facet re-renders only when its numbers move.
- Unlike a row watcher this maintains nothing: a commit re-runs the read. Over an
  index that costs O(groups); over a scan it costs the entity, which is the same
  bill `groupCount` pays and the reason a grouped surface is worth an index.
- No window and no coverage. `setWindow` is a no-op on the handle it returns.

#### Watching group heads — `watchGroupRows`

```ts
const view = client.watchGroupRows(Issue, q, (groups) => { ... }, { onError? });

// db facade
const view = client.db.issue.watchGroupRows(q, (groups) => { ... });
```

- Same `GroupRowsQuery` and the same answer as `groupRows` (§2), under
  `watchGroupCount`'s rhythm: re-read per commit, suppressed when the groups,
  their rows, their totals and their `hasMore` are all unchanged.
- Rows are decoded per read rather than shared with the entity's row watchers, so
  a callback may keep what it receives.

#### Watching rows by key — `watchRows`

```ts
type KeyedRows<T> = ReadonlyMap<string, Row<T> | undefined>;
type KeyedWatchCallback<T> = (rows: KeyedRows<T>, coverage: Coverage) => void;

interface KeyedWatchHandle {
	close(): void;
	setKeys(keys: Iterable<string>): void;
}

const view = client.watchRows(Todo, [ 't1', 't2' ], (rows) => { ... }, { include?, onError? });
view.setKeys([ 't2', 't3' ]);
```

- For "what is at these keys", where `watch()` would derive the whole entity to
  find them. One point read per key at subscribe and, per commit, one per watched
  key that commit named — no scan at any point, and nothing held in memory but
  the watched rows.
- Each callback is a **merge**, not a replacement: it carries the keys that
  moved, mapped to the row or to `undefined` where the key names no row. A key
  absent from the map has not changed. The first callback carries every key it
  was asked for, `undefined` included — "no such row" is an answer, not silence.
- `coverage` is the **entity's**. While `hydrating` is among its bounds an absent
  row may still be downloading; a coverage change alone fires a callback with an
  empty map, which is how that resolves.
- `setKeys` reads the added keys only — keys already watched keep their rows —
  and calls made within one turn are read as a single batch.
- A commit naming an entity without its rows re-reads every watched key.

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

const refs = [ { workspaceId: 'personal', key: 't1' }, { workspaceId: 'work', key: 't9' } ];
const view = client.watchRowsAll(Todo, refs, (rows) => { ... }, { include?, onError? });
view.setRefs([ ...refs, { workspaceId: 'work', key: 't10' } ]);
```

- `watchRows` addressed by bare key within one workspace; this names each row by
  workspace **and** key, so the refs may span every workspace the client holds
  and the same key in two of them is two independent rows. It is on the client,
  not on `client.ws(...)`.
- The cross-workspace counterpart of `watchQueryAll` for a set that comes from
  outside the entity — a pinned list, a feed, ids held elsewhere — where no
  predicate describes the membership.
- Deliveries are entries rather than a map, and merge exactly as `watchRows`
  does: a ref absent from a batch has not changed, and the first callback carries
  every ref it was asked for, absent rows included.
- One subscription and one batched read serve the whole set however many
  workspaces it spans. Per commit it re-reads only the watched refs that commit
  named; a commit naming an entity without its rows re-reads the refs **in that
  commit's workspace** and no others.
- `coverage` is the entity's across every workspace: complete only where it is
  complete in each of them.
- Rows are shared with the entity's other cross-workspace watchers, which is a
  different pool from the one the per-workspace watchers share: a row watched
  both ways is read and decoded twice.

#### Paging a live result — `setWindow`

`opts.window` replaces any `offset`/`limit` the query carries, and unlike them it
moves. The watch maintains the whole match set, so `setWindow` re-pages what it
already holds — a slice, not a read — and `rows.total` reports how many rows the
page was cut from. That is the pair a virtualized list needs: `total` sizes the
scroller, `setWindow` follows it.

```ts
const view = client.db.todo.watchQuery(
	{ orderBy: (t) => [ t.createdAtMs.desc() ] },
	(rows) => paint(rows, rows.total),
	{ window: { offset: 0, limit: 50 } },
);
onScroll((first) => view.setWindow({ offset: first, limit: 50 }));
```

A move landing on the same rows delivers nothing. `setWindow({})` delivers the
whole result.

`after`/`before` are a different mechanism and compose with this one: a watch's
cursors bound what the query MATCHES, and the window then slices what is left by
rank. So a live query with an `after` reports a `total` counting only the rows
past that position, and `setWindow` moves inside them. Every emitted page carries
`startCursor`/`endCursor` for the rows it delivered.

A watch honours `total: false` like a one-shot query — its pages report an
undefined `total` — so a query can move between the two paths without changing
what its consumer receives.

#### Deltas

The second callback argument describes the same change as an edit script against
the previous array. Ignoring it is always correct.

```ts
type WatchDelta<T> =
	| { kind: 'reset' }
	| { kind: 'splice'; changes: readonly RowSplice<T>[] };

interface RowSplice<T> {
	op: 'insert' | 'remove' | 'update'; // update replaces; the others shift
	index: number;
	row: Row<T>;
}
```

Apply the changes in order to a copy of the previous array and it becomes the
current one. `reset` means replace it wholesale — the first callback, a window
move, a commit that reported an entity without its rows, and **every** callback
on a watch with an `offset` or a `limit` (a page short enough to compare directly
is not worth translating indices for).

---

## 6. References & hydration

A reference field stores only the target's key fields; the live target is resolved
on read. All resolution is **orphan-tolerant** (missing target → `undefined`/empty).

### `include` (hydration)

`query`/`watchQuery` accept `{ include }`, an `Include` tree. Each result row
gets a parallel `refs` object (`HydratedRow`):

```ts
interface Include {
	[relation: string]: true | Include | …options…;
	$where?: (w) => Cond; // narrow the relation's rows
	$orderBy?: (w) => OrderKey[]; // order them
	$limit?: number; // page them — per holder, after $orderBy
	$offset?: number;
}
interface HydratedRow<T> extends Row<T> {
	refs: Record<string, HydratedRow | HydratedRow[] | undefined>;
}

const rows = await client.db.task.query(
	{ where: (t) => t.done.eq(false) },
	{ include: { project: true, blockedBy: { project: true } } },
);
rows[0].refs.project; // HydratedRow | undefined (to-one)

// "each project with its 5 most recent open tasks"
const projects = await client.db.project.query({}, {
	include: {
		tasks: {
			$where: (t) => t.done.eq(false),
			$orderBy: (t) => [ t.createdAt.desc() ],
			$limit: 5,
		},
	},
});
```

`$`-prefixed keys are options, everything else is a relation — no proto field or
derived relation name can start with `$`. Options are evaluated against the
relation's **target** entity with query semantics, and they narrow that
relation's list for each holder independently: `$limit: 5` is five rows per
holder, not five overall. They need a relation that resolves to a list (a
reverse relation or a to-many ref); hydration rejects them on a to-one ref, and
at the tree root. The option callbacks receive an untyped field reference —
relation names are plain strings, so there is no schema to infer the target's
fields from. A subscription's `follow` stays an option-free `IncludeSpec`
(`[relation: string]: true | IncludeSpec`): the follow closure syncs rows, it
does not shape a result.

Hydration is batched: forward targets resolve as point reads deduplicated
across the call, a reverse relation costs one query per workspace the result
spans (never one per row), and rows referencing one target through the same
relation share the hydrated object — so nested includes resolve once per
distinct target. Do not mutate delivered rows; a shared target makes the edit
visible through every holder.

### Explicit ref accessors

Forward refs are typed from the message shape; reverse relations are not, since
their names are coined by the registry from the ref graph and the generated
types know nothing about them:

```ts
// forward ref → resolve live target(s), typed to the target
client.db.task.refs.project.load(holder):   Promise<Row<Project> | undefined>
client.db.task.refs.blockedBy.load(holder): Promise<Row<Task>[]>
// reverse relation → source rows pointing here (optional narrowing query)
client.db.project.refs.tasks(target, q?): Promise<Row[]>

// schema-first equivalents
client.resolveRef(holderSchema, fieldLocalName, holder)
client.resolveReverseRef(targetSchema, relationName, target, q?)
```

A reverse relation onto a **single-key** target through a **to-one** ref
resolves as a pushed-down predicate on the source's ref leaf. Through a to-many
ref, or onto a composite-key target, there is no comparable scalar leaf (a
composite ref is `isSet`-only in the query DSL), so it scans the source entity
and compares derived keys instead — `limit`/`offset` then page the matched rows.
The server's reverse-follow hop is an unindexed scan for the same reason, so the
two agree on both results and cost.

The surface keeps a string index signature for the reverse names, so an unknown
name still type-checks — it throws at access naming the known accessors. That
includes a message field whose target is not a registered entity: it looks like
a forward ref to the type system, because reference-ness lives in a proto option
the TS types do not carry.

Types: `EntityRefs<D>`, `ForwardRefAccessor` (`{ load(holder) }`), `ReverseRelAccessor`
(`(target, q?) => Promise<Row[]>`).

---

## 7. Undo / redo

Enable via the `undo` option. In-memory only (per instance, dies with the page).

```ts
const client = new SyncClient({ ..., undo: { enabled: true, maxDepth: 100 } });

await client.undo(): Promise<boolean>   // false if nothing to undo
await client.redo(): Promise<boolean>
client.canUndo(): boolean
client.canRedo(): boolean
client.onUndoChange(cb: () => void): () => void   // fires after any change to canUndo/canRedo
```

```ts
interface UndoOptions {
	enabled?: boolean;
	maxDepth?: number;
} // enabled defaults true when object present; maxDepth default 100
```

Semantics: PUT/PATCH/DELETE invert against the pre-mutation row; a `transact()` batch
is one undo group (replays atomically); custom mutations are a **barrier** (both
stacks clear); a new user edit clears the redo stack; a rejected mutation drops its
whole group. Undo is driven entirely through the client — `undo()`, `redo()`,
`canUndo`, `canRedo`, `onUndoChange` and the `useUndo` hook; the manager behind
them is internal.

### Nested undo scopes

A workspace's stack is shared by everything writing to it. For UI whose undo must
stay local — a task dialog, an inline editor — fork a scope:

```ts
const dialog = client.ws('w1').undoScope({ maxDepth: 200 }); // or client.undoScope()

await dialog.db.task.mutate.patch(id, { priority: 2 }); // recorded on the dialog's stack
const title = await dialog.text(TaskSchema, id, 'title'); // so are its text edits
await dialog.undo(); // undoes the dialog's last step only

dialog.close(); // drops the stack; the workspace's is untouched
```

`UndoScope` is a full `WorkspaceClient` plus `close()` and `closed`, so every
write verb routes by which object issued it — no per-call option to thread.
Everything else is shared: one store, one subscription set, one pending queue,
one push.

|                             |                                                                                                                                                   |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Nesting                     | Innermost wins; a scope forked off a scope forks the workspace, it does not chain.                                                                |
| Custom mutation             | Clears **every** stack in the workspace — it is opaque and may have touched anything.                                                             |
| Rejection / compaction      | Reach every live stack in the workspace, the scopes' included.                                                                                    |
| `close()`                   | Drops both stacks; NOT merged into the parent. The object stays usable and records nothing, so a callback firing during teardown is not an error. |
| `undo()` after close        | Resolves `false` (nothing to undo) rather than throwing.                                                                                          |
| Undo disabled on the client | A scope is inert, exactly like the workspace it forked.                                                                                           |
| `maxDepth`                  | Per scope; defaults to the client's `undo.maxDepth`.                                                                                              |

---

## 8. Sync status

```ts
interface SyncStatus {
	// Account-wide: one stream, one server build.
	state: 'idle' | 'syncing' | 'error';
	online: boolean; // live stream connected, or the last cycle succeeded
	dirty: boolean; // derived: pendingMutations > 0, anywhere
	pendingMutations: number; // total across every workspace
	lastSyncedAt?: Date;
	lastError?: SyncErrorInfo; // { message, code?, transportCode? }
	schemaSkew?: SchemaSkewInfo; // { client, server, action }; a standing condition
	// Per workspace, one entry each. Also reachable as client.ws(id).status().
	workspaces: Record<string, WorkspaceStatus>;
	// The default workspace's fields, mirrored so a single-workspace app never
	// indexes `workspaces`. Unset when there is no default, or it has not synced.
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

type WorkspaceAccess = 'granted' | 'revoked';
type SchemaSkewAction = 'ignore' | 'warn' | 'block' | 'resync';

interface SyncScopeInfo {
	own: number; // subscriptions this client declared
	effective: number; // what the coordinator syncs, peer tabs included
	widened: boolean;
}
interface HydrationInfo {
	pending: readonly string[]; // entities still arriving, in delivery order
	total: number; // entities the snapshot ships
	done: number; // how many have landed in full
}
```

`hydration` is present only on a **cold** start, and only until the snapshot is
complete — a re-bootstrap swaps atomically, so nothing is ever partly visible.
Per read, the same fact arrives as the `hydrating` coverage bound (§9).

`RejectionInfo` is delivered to `onMutationRejected` (carries `mutationId` and reason;
codes include `"conflict"`, `"poison_dropped"`). `CodePoisonDropped` and the
proto codes `CodeConflict`, `CodeInvalidArgument`, `CodePermissionDenied` are exported.

Which channel reports which failure, and the full code vocabulary, are in
[Sync status and failure codes](sync-status-and-codes.md).

### Devtools — `attachDevtools(client, opts?)`

Console-first debugging over one client: the state a "why doesn't this row show
up" investigation needs, without wiring any callbacks in advance. Purely
observational — it writes nothing and changes no sync behavior.

```ts
import { attachDevtools } from 'syncplane';

attachDevtools(client); // published as globalThis.__syncplane

// later, in the browser console:
await __syncplane.snapshot(); // one report over everything below
await __syncplane.explain('todo', 't42'); // why is this row (not) visible?
__syncplane.rejections(); // rejection buffer, oldest first
__syncplane.events(); // SyncEvent buffer, oldest first
```

`snapshot()` reports the sync status, stream leadership (`thisTab`, and
`heldByAnyTab` where Web Locks can answer), each workspace's pending queue
(decoded: targets, mutator, whether a push has carried it) and pushed-through
watermark, per-entity coverage, every row holding optimistic state (overlay
entries, tombstones, queued text edits), both buffers, the text writer
identities held by this tab's open text handles (and device-wide, via Web
Locks), and every open room with its roster.

`explain(entity, key, workspaceId?)` — entity by name or schema, workspace
defaulting to the client's — answers with the row's local presence, overlay
state, queued mutations and retained rejections targeting it, its entity's
coverage, and a one-sentence `verdict` distinguishing "deleted locally",
"visible optimistically", "rejected and rolled back", "outside the sync scope"
and "the server holds no such row".

Options: `eventBufferSize` (default 200), `rejectionBufferSize` (default 100),
`expose` — the `globalThis` name, or `false` to publish nothing. The buffers
start at attach, so attach next to `new SyncClient(...)`; `detach()` unsubscribes
and withdraws the global. Snapshots carry `bigint` mutation ids, so
`JSON.stringify` needs a replacer for them.

---

## 9. Partial sync — subscriptions

```ts
import { subscription } from 'syncplane';

const specs = [
  subscription(Task, (t) => t.archived.eq(false), {
    include: { project: true },  // follow refs into scope
    depth: 2,                     // transitive follow rounds
  }),
  // Windowed: the 100 highest-priority open issues, and nothing else.
  subscription(Issue, (i) => i.state.eq('open'), {
    limit: 100,
    orderBy: (i) => [ i.priority.desc() ],
  }),
  // Windowed by write sequence: the 500 most recently changed comments.
  subscription(Comment, undefined, { limit: 500, byRecency: true }),
];
new SyncClient({ ..., subscriptions: specs });
client.setSubscriptions(specs);  // runtime swap → server delta-resyncs the difference
```

```ts
interface SubscriptionSpec {
	schema: DescMessage;
	where?: (w) => Cond;
	follow?: IncludeSpec;
	followDepth?: number;
	orderBy?: (w) => OrderKey[];
	limit?: number;
	byRecency?: boolean;
}
function subscription<D>(
	schema,
	where?,
	opts?: { include?; depth?; limit?; orderBy?; byRecency? },
): SubscriptionSpec;
```

Empty/absent subscriptions = full sync. All tabs sharing a store sync the UNION of
live subscription sets (full sync is absorbing) — see `SyncStatus.scope`.

Which is why subscriptions cannot be used to keep an entity off clients: one tab
that never calls `setSubscriptions` pulls the whole catalog into the shared
store and every sibling inherits it. An entity that must never reach a client is
declared [`server_only`](proto-options.md#server-only-entities) in the schema and
read with `queryServer`.

Such an entity is still registered in `entities` — that is how `queryServer`
resolves it — but naming it in a subscription set throws, since it would sync
nothing. An app deriving its subscriptions from its entity list has to leave it
out.

### Windows — capping a subscription by rank

`limit` is what makes a subscription windowed: only the first `limit` rows of
the predicate match sync, ranked by `orderBy` (or by write sequence, newest
first, with `byRecency`), ties broken on row key. A row that falls off the tail
is removed from the local store — the cursor carries the window's membership as
a key-set and every pull diffs a freshly computed window against it, so a row
that merely sank below the boundary is dropped as precisely as a deleted one.

- `orderBy` or `byRecency` without a `limit` throws; so does a non-positive or
  non-integer `limit`, and setting both `orderBy` and `byRecency`.
- The server rejects a `limit` above `MaxWindowSize`
  ([default 1000](server.md)).
- Changing the window delta-resyncs the difference; changing `follow` alone does
  not.
- A windowed entity is never `complete`: results report `bounds: ['window']`
  and, when a window is the only thing scoping the entity, the total `limit`.
  A client holding the top N by one order cannot answer a local query ordered by
  another — check `coverage` and fall back to `queryServer`.

A window bounds the client, not the server: computing an `orderBy` window costs
what the equivalent `Query` costs. Why they exist and how membership is
maintained: [Partial sync and coverage](../explanation/partial-sync-and-coverage.md).

### Coverage

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
	limit?: number; // present when a window is the only thing scoping the entity
}
```

Every result array carries one as a non-enumerable `coverage`, beside `total`
(see §2). `hydrating` is the
transient bound: a cold client's first snapshot lands entity by entity, so an
entity that has not arrived reads as `[]` exactly like an empty one until this
says otherwise. It clears on its own and the readers re-fire.

`relation` appears only on queries that cross a reference (`matches()`): a
holder row whose target never synced evaluates against a missing row and reads
as a non-match. The bound is absent when the crossed reference is provably
answerable — the target entity's coverage is complete in the workspace, **or**
every subscription scoping the queried entity `follow`s the crossed ref field,
which puts each locally-held holder's targets in the follow closure. A
`matches` through a ref nested inside a message can only be covered the first
way, since a follow path names top-level relations.

### Delivery order

```ts
new SyncClient({ ..., bootstrapPriority: [ Issue, Comment ] });
```

Names the entities the first snapshot delivers first — what the first screen
renders. A reordering, never a filter: everything else follows in the server's
own order (a subscribed client that names none is served in `subscriptions`
order). Only cold starts feel it.

---

## 10. TS-first schema builder (`schema` namespace)

Alternative to authoring `.proto` + `buf`. Declare entities in TS; the builder
synthesizes the same descriptors the engine speaks. Imported as a namespace:

```ts
import { schema } from 'syncplane';

const todo = schema.entity('todo', {
  id:        schema.text().key(),
  title:     schema.text(),
  done:      schema.bool(),
  createdAt: schema.int64(),
  tags:      schema.list(schema.text()),
  settings:  schema.message({ color: schema.text(), size: schema.int32() }),
  blob:      schema.bytes().opaque(),
  items:     schema.keyedList(schema.message({ itemId: schema.text(), pos: schema.text() }), 'itemId').order('pos'),
});

type Todo = schema.Infer<typeof todo>;          // inferred static row type

const bundle = schema.defineSchema(todo);
const client = new SyncClient({ entities: bundle.descMessages(), ... });
```

### Field constructors

| Fn                              | Type                 | Notes                                                               |
| ------------------------------- | -------------------- | ------------------------------------------------------------------- |
| `text()`                        | `Field<string>`      | proto `string`                                                      |
| `bool()`                        | `Field<boolean>`     |                                                                     |
| `int32()`                       | `Field<number>`      |                                                                     |
| `int64()`                       | `Field<bigint>`      |                                                                     |
| `uint64()`                      | `Field<bigint>`      |                                                                     |
| `double()`                      | `Field<number>`      |                                                                     |
| `bytes()`                       | `Field<Uint8Array>`  | `.opaque()` for atomic blob                                         |
| `list(inner)`                   | `Field<T[]>`         | repeated scalar                                                     |
| `message(fields)`               | `Field<RowOf<F>>`    | embedded sub-message; per-field LWW; `.opaque()` to replace whole   |
| `ref(() => target)`             | `Field<Infer<E>>`    | typed to-one reference (thunk → self/forward refs)                  |
| `refList(() => target)`         | `Field<Infer<E>[]>`  | to-many reference                                                   |
| `keyedList(elem, ...keyFields)` | `KeyedListField`     | repeated message merged per item; `.order(posField)` → movable list |
| `map(keyType, value)`           | `Field<Record<...>>` | map; the value is a scalar or a `message()`                         |

Field modifiers: `.key()` (singular `text`/`bool`/int scalar only; ≥1 required per
entity — see [Naming rules](naming.md)), `.opaque()`, `.indexed()`,
`.order(posField)` (on `keyedList`).

### Secondary indexes

`.indexed()` maintains an index over one field, so filtering or ordering by it
costs O(result) rather than a scan — on the server, and on the client for both
`query()` and the first read of a `watchQuery()`. For a query that filters on
one field and orders by _another_, declare a composite instead — a sorted
key-value store has no posting lists to intersect, so one marked field can only
serve that shape by re-checking every candidate:

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

Paths are accessor names, dotted for nested fields (`'settings.size'`) — the
same form a query path takes. An index path may also run through a repeated
message field (`'lineItems.sku'`), which is what makes an `any()` on that field
a range read; the row then takes one position per element. Components of one
index may only fan out over a single list, since a second would pair values
belonging to different elements. An index name must not start with `__`, which
is reserved for indexes the engine derives for itself.

`.indexed()` on a `list()` of scalars is a multi-entry index: the row takes one
position per element, which is what makes `has`/`hasAny` a range read. The write
cost grows with the list, since every write re-stores every element's position.

`.indexed()` is rejected on a message list (no comparison reaches it) and on a
`ref()` (the engine maintains one over every reference field already, which is
what makes a reverse lookup a range probe). It is rejected on a `map()` too, but
for a different reason: a field mark has no room to say which entry or which
leaf, so a map is indexed through the `*` form below. Registration additionally
rejects a collaborative-text field, whose entry every keystroke would rewrite.

A fan-out component serves an ordering only where the equality prefix has
already pinned it — `{ key: [{ path: 'labels' }, { path: 'rank', desc: true }] }`
serves "labelled X, newest first" at O(limit), because each matching row sits at
exactly one position under that label.

A `*` segment fans out over every entry of a **map**, which is how user-defined
fields are indexed, and the segment after it — `key` or `value` — says which side
of the entry the component stores. One declared path is one component, so an
index states its own layout:

```ts
indexes: [
	// One named custom field: equality, ranges and an ordering within it.
	{
		name: 'cf_number',
		key: [ { path: 'custom.*.key' }, { path: 'custom.*.value.number' } ],
	},
	{
		name: 'cf_num_rank',
		key: [
			{ path: 'custom.*.key' },
			{ path: 'custom.*.value.number' },
			{ path: 'rank', desc: true },
		],
	},
	// The mirror layout: which custom field holds X. Equality only.
	{
		name: 'cf_by_number',
		key: [ { path: 'custom.*.value.number' }, { path: 'custom.*.key' } ],
	},
	// nested: one entry per (key, element) — multi-select
	{
		name: 'cf_labels',
		key: [ { path: 'custom.*.key' }, { path: 'custom.*.value.labels.values' } ],
	},
];
```

`[key, value]` clusters one field's values, so it serves equality, ranges and an
ordering within that field. `[value, key]` clusters one value, so it serves the
cross-key `any()` above — and a named-key equality too, by pinning the value and
then the key. An app that only does equality probes declares **one** index, the
value-first one. Declaring both roughly doubles the entries a row occupies; see
[limits and trade-offs](../explanation/limits-and-trade-offs.md).

The selector is required rather than defaulted: for a `map<string, string>` a
bare `attrs.*` could mean either side of the entry, with no error to notice.

Unlike a list fan-out, the value under a **pinned** entry key IS an ordering: a
map holds at most one value per key, so the row sits at exactly one position
there. An ordering by a custom field that no predicate pins declines to the
scan, because rows lacking that key sit outside every key range and must sort
last. A literal key in an index path is rejected — an index names a shape, and
indexing one entry is a different feature.

A value component with no key component beside it is legal, and is what serves a
cross-key `any()`; a predicate naming one field then stays in the residual, since
the range spans that value under every key.

Indexes are invisible: no request or response names one, a query returns the
same rows in the same order whether or not one served it, and adding or removing
one is never a breaking change. A new index is unusable until a background pass
has walked the entity, during which queries scan. The cost is on writes — every
write to the entity re-stores that row's position in every index it has, unless
its field mask cannot reach any of them.

What the client index does and does not cover:

- **`query()`** uses it when the query is scoped to one workspace and the
  planner can express its predicate and ordering as a range — or, for a
  top-level `or`, as one range set per branch, taken only when every branch
  has an index. A cross-workspace query always scans.
- **`watchQuery()`** uses it for its first read, under the same planner as
  `query()`. It asks for every match rather than one page — that set is what it
  goes on to maintain — so a query whose matches run into the thousands can
  exceed the walk budget and scan where the same query passed to `query()` with
  a `limit` would not. Later commits read only the rows they name, and a rescan
  (see Watchers) scans.
- **Confirmed rows only.** The optimistic overlay is scanned whole and merged
  over the index range, which is O(pending) and keeps the answer identical to
  the merged view.

Which path a read actually took is reported through `onSyncEvent`: every local
read emits a `query` event carrying `path`, the index that served it, and the
reason for a decline. See [Telemetry](telemetry.md#local-reads).

Set `debugVerifyIndexReads` (or `SYNC_DEBUG_VERIFY_TOUCHES=1`) to run both paths
on every query and throw if they disagree.

### Full-text search

`.searchable()` marks a singular `text()` field as searched; the entity-level
`search` option adds fields in importance order (marked fields count ahead of
it) and analyzer options:

```ts
const article = schema.entity('article', {
	id: schema.text().key(),
	title: schema.text().searchable(),
	summary: schema.text(),
}, {
	search: { fields: [ 'summary' ], keepDiacritics: false },
});
```

`.searchable()` throws on anything but a singular `text()`. The declaration is
what `match(...)` reads and what `relevance()` ranks by, and it also declares
the term index the previous section's rules apply to — built in the
background, invisible to results, maintained on every write. See
[proto options](proto-options.md#full-text-search) for the .proto form and
[Search data](../how-to/search.md) for querying.

### Write policies

The same options object carries `createPolicy`, `updatePolicy` and
`deletePolicy`, which decide who may write rows of the entity. Each defaults to
`'allow'`:

```ts
const comment = schema.entity('comment', {
	id: schema.text().key(),
	body: schema.text(),
}, { updatePolicy: 'trustedOnly', deletePolicy: 'trustedOnly' });
```

`'trustedOnly'` takes that write away from clients entirely, leaving it to
custom mutators — whose rows are covered by the verdict on the invocation — and
to trusted server-side writes.

The client enforces this locally: `put`, `patch`, `del` and everything built on
them (`update`, the typed `db` facade) **throw** for a write the schema forbids,
before it reaches the pending queue. A forbidden write therefore never applies
to the optimistic overlay and is never rolled back, and the error names the
entity and the option that refused it.

Which policy a write must satisfy follows from the write alone. A `patch` is an
update. A `del` is a delete. A `put` carrying `{ expectedVersion: 0n }` is a
create, one carrying any other version is an update, and one carrying none could
land either way — so it needs create and update to be permitted both. Creating a
row of an append-only entity therefore takes an explicit assertion:

```ts
await client.mutate.put(CommentSchema, { id, body }, { expectedVersion: 0n });
```

See [Write policies](proto-options.md#delete-policy) for the full semantics.

### Protected fields

`.protected()` puts one field of an otherwise writable row out of a client's
reach — a status a server-side workflow advances, an owner stamped at creation:

```ts
const task = schema.entity('task', {
	id: schema.text().key(),
	title: schema.text(),
	status: schema.text().protected(),
});
```

A `patch` whose mask reaches the field **throws** at the call site, like a
forbidden write policy. A `put` does not: it names no field, so the field keeps
the value the local row holds (its zero value when there is none) and the rest of
the write lands — which is what the server does with the row it has. `update`
follows `patch`, so a recipe that assigns to a protected field throws.

Valid on any field but a key. A oneof is protected as a group, and a mask is
matched by its first segment, so protecting a message field also refuses masks
reaching into it.

The server can protect further fields per caller, and those are invisible here: a
write naming one enqueues and comes back as a `permission_denied` rejection ack.
See [Protected fields](proto-options.md#protected-fields).

> **Field order is significant** — fields get proto numbers 1..N in declaration order.
> Evolution is append-only; reordering/removing breaks wire compatibility.

### Compilation — `defineSchema(...entities): SchemaBundle`

```ts
interface SchemaBundle {
	entities: readonly EntitySchema[];
	toFileDescriptorSet(): FileDescriptorSet;
	toBinary(): Uint8Array; // wire form a Go server ingests at runtime
	toRegistry(): FileRegistry;
	descMessages(): DescMessage[]; // for new SyncClient({ entities })
	descMessage(name: string): DescMessage;
}
```

Also: `entitiesFromDescriptorSet(bytes): DescMessage[]` rebuilds descriptors from
bytes a server handed back. Exported types: `EntitySchema`, `Fields`, `Field`,
`KeyedListField`, `Infer`, `RowOf`.

### Admin (schema registration)

```ts
import { registerSchema, fetchSchema } from 'syncplane';

await registerSchema(transport, appId, bundle, { token?, expectedRevision? }): Promise<RegisterResult>;
// RegisterResult: { revision, status: 'created'|'updated'|'unchanged', entityNames }
await fetchSchema(transport, appId): Promise<DescMessage[]>;
```

---

## 11. KV stores

```ts
import {
	IDBKVStore,
	type KVRead,
	type KVStore,
	type KVWrite,
	MemoryKVStore,
} from 'syncplane';
```

- `IDBKVStore` — IndexedDB (default in browsers); multi-tab safe via Web Locks.
- `MemoryKVStore` — in-memory (default off-browser).
- Pass an instance as the `kvStore` option to override the default.
- `KVStore` / `KVRead` / `KVWrite` are the interfaces a custom backend implements.

A backend is a key-ordered map plus **one secondary index**. Confirmed rows carry
the index positions they occupy as a string array under the field `ix`
(`INDEX_FIELD`), and `KVRead.indexScan(lo, hi)` yields the keys of the records
holding a position in `[lo, hi)`, in position order, one yield per position. That
is what makes a client index cost no storage record of its own — the positions
ride on a row that is written anyway — and it is the one method beyond the map a
custom backend has to provide. It must be lazy: an index read usually wants the
first few rows of a wide range, and stopping the iteration must stop the work.

### Storage names

```ts
import {
	DEFAULT_STORAGE_PREFIX,
	type StorageNames,
	storageNames,
} from 'syncplane';
```

Every name the client claims on the origin is `<storagePrefix>` plus the account,
and `client.storageNames` reports the set it resolved:

| Field           | Namespace        | Default name                             |
| --------------- | ---------------- | ---------------------------------------- |
| `store`         | IndexedDB        | `sync:<accountId>`                       |
| `kvLock`        | Web Locks        | `sync:<accountId>`                       |
| `streamLock`    | Web Locks        | `sync:stream:<accountId>`                |
| `changeChannel` | BroadcastChannel | `sync:change:<accountId>`                |
| `reopenChannel` | BroadcastChannel | `sync:reopen:<accountId>`                |
| `textPeerLocks` | Web Locks        | `sync:textpeer` (+ `:<clientId>:<slot>`) |

Changing `storagePrefix` points the client at a fresh, empty database: it
bootstraps from the server and never reads the old one, so anything still pending
there is not pushed. The abandoned database stays on disk until the app deletes
it — `storageNames(oldPrefix, accountId).store` names it for
`indexedDB.deleteDatabase`.

---

## 12. React layer (`syncplane/react`)

`react` is an optional peer dependency. The core build is JSX-free, and the
entry carries a `'use client'` directive so React Server Components builds can
import it directly.

```tsx
import { createReactSync } from 'syncplane/react';
import { TodoSchema } from './gen/.../todo_pb.js';

const entities = [ TodoSchema ] as const;
const sync = createReactSync(entities);

function App() {
	return <SyncProvider options={ { url, workspaceId, entities } }>
		<Todos />
	</SyncProvider>;
}

function Todos() {
	const todos = sync.db.todo.useQuery({ where: (t) => t.done.eq(false) });
	const status = sync.useSyncStatus();
	const { canUndo, undo } = sync.useUndo();
	const m = sync.db.todo.useMutate();
	// m.put / m.patch / m.update / m.del
}
```

### `createReactSync(entities): ReactSync<E>`

```ts
interface ReactSync<E> {
	SyncProvider: typeof SyncProvider;
	WorkspaceProvider: typeof WorkspaceProvider;
	db: ReactEntityApis<E>; // per-entity hooks keyed like client.db
	useSyncStatus: typeof useSyncStatus;
	useWorkspaceStatus: typeof useWorkspaceStatus;
	useUndo: typeof useUndo;
}
```

### Per-entity hooks — `sync.db.<entity>`

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
	// Across workspaces: every one the client holds, or opts.workspaces.
	// `q` may carry `byWorkspace` in place of `where` (see §2).
	useQueryAll(
		q: Query<MessageShape<D>> | WorkspaceQuery<MessageShape<D>>,
		opts?: UseQueryAllOptions,
	): WatchResult<MessageShape<D>>;
	// Counted groups of the match set — a column badge, a facet sidebar (see §2).
	useGroupCount(
		q: GroupQuery<MessageShape<D>>,
		opts?: UseWorkspaceOptions,
	): GroupResult;
	// The distinct values behind those groups — what a filter dropdown offers.
	useDistinct(
		q: GroupQuery<MessageShape<D>>,
		opts?: UseWorkspaceOptions,
	): DistinctResult;
	// The head of each group — a board's columns, each with its first rows (§2).
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
	useMutate(opts?: UseWorkspaceOptions): EntityMutate<D>; // { put, patch, update, del }
	useText(key: string, field: string, opts?: UseWorkspaceOptions): UseTextResult;
}

// One slot per requested key, in the order asked for, plus the same load state.
type UseRowsResultFor<D> =
	& readonly (Row<MessageShape<D>> | undefined)[]
	& WatchState;

// The groups client.groupCount returns, plus how the watch behind them is
// doing. There is no `hydrating`: a grouped read reports no coverage, so read a
// useQuery beside it when a UI has to tell a downloading entity from an empty
// one.
type GroupResult = readonly Group[] & Omit<WatchState, 'hydrating'>;
type DistinctResult =
	& readonly (Scalar | undefined)[]
	& Omit<WatchState, 'hydrating'>;
type RowGroupResult<T> = readonly RowGroup<T>[] & Omit<WatchState, 'hydrating'>;

// The array client.query returns, plus how the watch behind it is doing.
type WatchResult<T> = ReadonlyRowSet<T> & WatchState;
interface WatchState {
	readonly loading: boolean; // true until the first callback; always false under suspense
	readonly hydrating: boolean; // this entity's rows are still arriving in the first snapshot
	readonly error: Error | undefined; // the watch could not be established; no rows will arrive
}
```

- `loading`/`hydrating`/`error` are non-enumerable, exactly like `coverage`:
  `toEqual([])`, `Object.keys` and `JSON.stringify` do not see them.
- `hydrating` mirrors `coverage.bounds.includes('hydrating')`. It is what
  `loading` cannot say: a cold client fills entity by entity, so a result can be
  empty because its entity has not started downloading. Guard an empty state with
  `loading || hydrating` — under suspense, which never reports `loading`, with
  `hydrating` alone.
- `useRow` returns `{ row, loading, hydrating, error, patch(init, opts?), update(recipe, opts?),
  del(opts?) }`. The writers go through per-field LWW and no-op while `row` is
  undefined; **`{ cas: true }`** binds the row's current `version` as
  `expectedVersion` (an explicit `expectedVersion` wins over it). It is opt-in
  because `Row.version` is confirmed-only, so a guard bound to it makes a burst
  of edits on one row conflict with its own unacked writes — see
  [Compare-and-swap from a row hook](../how-to/react.md#compare-and-swap-from-a-row-hook).
  Reach for `update` to save an edit to the row the hook just handed back —
  patching `row` itself is a compile error, for the reason in
  [Which write verb](#which-write-verb).
- `useRows` returns one slot per requested key, in the order given, `undefined`
  where the key names no row. The array is always that length, including before
  anything has been read, so `rows[i]` names `keys[i]` from the first render;
  `loading` stays true until every key has been answered. `keys` may be a fresh
  array every render and is not retained. The array keeps its identity while
  nothing in it moves.
- Every `useRow` and `useRows` on one (workspace, entity, include) shares a
  single subscription, which reads exactly the keys they hold — one point read
  per key at mount and, per commit, one per watched key that commit named. A
  commit re-renders only the hooks whose own row moved, and nothing about a row
  hook scales with the size of the entity.
- `useRowsAll` is `useRows` over `RowRef`s (`{ workspaceId, key }`) instead of
  bare keys, so one call site holds rows from several workspaces — the set no
  `useQueryAll` predicate describes. Result shape, ordering and load state match
  `useRows`; `opts` is `{ include }` only, the enclosing `<WorkspaceProvider>` is
  ignored, and there is no suspense flavor — all matching `useQueryAll`. Every
  `useRowsAll` on one (entity, include) shares one cross-workspace subscription,
  separate from the per-workspace one above: a row read through both is read
  twice. Writes route back through `client.ws(row.workspaceId)`; see
  [`watchRowsAll`](#watching-rows-across-workspaces--watchrowsall).
- **`opts.window`** (`{ offset?, limit? }`) pages the result the subscription
  already holds, so moving it costs a slice rather than a re-read and does not
  resubscribe — the virtualization path, with `rows.total` sizing the scroller. An
  edit the page cannot see does not re-render. `useSuspenseQuery` is the
  exception: there the window is part of the subscription identity (one cached
  entry serves every call site with that identity, and two windows would fight
  over it), so a move re-reads and re-suspends.
- The `useSuspense*` variants wait for the first callback rather than reporting
  `loading`, and throw a failed subscription for an error boundary. Suspense is a
  separate hook, not an option, because it selects a fixed hook path.
- `UseWorkspaceOptions`: `{ workspaceId?: string; undo?: 'nearest' | 'workspace' }` —
  every hook accepts it. `UseWatchOptions`/`UseQueryOptions`/`UseRowOptions` add
  `{ include?: Include }`. Option lambdas (`$where`/`$orderBy`) fold into the
  subscription identity by what they return, so a new function per render does
  not resubscribe — a structurally different condition does.

### Workspace resolution

Every hook targets one workspace, resolved as: the `{ workspaceId }` option →
the nearest `<WorkspaceProvider workspaceId>` → the client's `defaultWorkspace`.
A client with several workspaces and no default throws from the hook until one
of the first two names a workspace. Reads, writes, text handles, undo and
`useWorkspaceStatus` all follow it; `useSyncStatus` is account-wide.

### Top-level hooks / components

```ts
useSyncClient(): SyncClient                       // from context
useWorkspaceScope(workspaceId?): WorkspaceClient  // the resolved workspace
useWorkspaceId(): string | undefined              // what WorkspaceProvider set, if anything
useSyncStatus(): SyncStatus                       // reactive, account-wide
useWorkspaceStatus(opts?): WorkspaceStatus        // reactive, one workspace
useUndo(opts?): { canUndo, canRedo, undo(), redo() }
useUndoScope(opts?): UndoScope                    // a scope with the component's lifetime
useNearestUndoScope(): UndoScope | undefined      // what UndoScopeProvider set, if anything
useOnMutationRejected(handler: (info: RejectionInfo) => void, opts?): void
<SyncProvider options={SyncClientOptions}>        // provider constructs, starts and closes it
<SyncProvider client={SyncClient}>                // caller-built; caller keeps the lifecycle
<WorkspaceProvider workspaceId={string}>          // re-scopes the hooks below it
<UndoScopeProvider maxDepth?={number}>            // gives the subtree its own undo stack
```

`<UndoScopeProvider>` opens a [nested undo scope](#nested-undo-scopes) for its
subtree: writes below it (mutate hooks, `useText`, transactions) record there,
and `useUndo()` below it drives that stack. The scope's lifetime is the
subtree's — mounting opens it, unmounting closes it and drops the stack. One
write can opt back out with `{ undo: 'workspace' }`, for the action inside a
dialog that belongs to the view behind it. Reads are unaffected either way: a
scope and its workspace share one subscription.

```tsx
<UndoScopeProvider>
	<TaskDialogBody id={ id } /> // its useUndo() is the dialog's history
</UndoScopeProvider>;
```

`useOnMutationRejected` is the React face of the dead-letter surface (also
`client.onMutationRejected(cb)`, the multi-listener sibling of the
`onMutationRejected` option). It filters to the hook's resolved workspace unless
`{ allWorkspaces: true }`, and holds the handler in a ref so an inline arrow does
not resubscribe. `RejectionInfo.targets` names the rows the dead mutation would
have written — one entry for a CRUD mutation, one per member for a batch, none
for a custom mutator (which `RejectionInfo.mutator` names instead).

`SyncProvider` takes exactly one of `options` and `client`, and throws if given
both or neither. With `options` it is StrictMode-safe (the deferred close is
cancelled by the remount), reads `options` once, and reports a failed `start()`
to `onStartError` — or, without one, by throwing during render for the nearest
error boundary.

---

## 13. Other top-level exports

- `newId(): string` — fresh time-ordered string id (same generator `create` uses).
- Fractional indexing: `generateKeyBetween(a, b, clientId?)`,
  `generateNKeysBetween(...)`, `BASE_62_DIGITS`.
- Custom mergers ([reference](proto-options.md#custom-mergers-for-opaque-fields)):
  types `MergerSpec` and `MergeFn`, for the `mergers` client option. The entity
  registry, the field-mask merge and the reference plumbing they run on are
  internal.
- Naming rules ([reference](naming.md)): `validateEntityName`, `validateKeyField`,
  `validateRowKey`, `MAX_ENTITY_NAME_LENGTH`, `MAX_ROW_KEY_BYTES` — the same
  checks writes and registration run, for an app minting its own keys.
- Storage names ([above](#storage-names)): `storageNames`, `DEFAULT_STORAGE_PREFIX`
  and the `StorageNames` type, for an app that changed `storagePrefix` and has to
  name what the old one left behind.
- `subscription` builder + `SubscriptionSpec` type.
- Observability ([reference](telemetry.md#the-typescript-client)): `SyncEvent` and
  the member types (`PushEvent`, `PullEvent`, `BootstrapEvent`, `RebaseEvent`,
  `PokeEvent`, `StreamEvent`, `LeadershipEvent`, `ResequenceEvent`, `QueryEvent`),
  plus `SyncEventHandler`, `SyncEventOutcome` and `QueryDeclineReason`, for typing
  an `onSyncEvent` handler. Types only — events are delivered, never constructed.
- Rooms ([below](#14-rooms--presence-and-broadcast)): `jsonCodec` and
  `RoomSendError`, plus the `Room`, `Peer`, `RoomCodec`, `RoomError`,
  `RoomMessage`, `RoomOptions` and `RoomStatus` types.
- Error codes: `CodeConflict`, `CodeInvalidArgument`, `CodePermissionDenied`,
  `CodePoisonDropped`, `CodeRoomDropped`, `CodeRoomOffline`, `CodeRoomClosed`.

---

## 14. Rooms — presence and broadcast

The ephemeral plane: state that only matters while somebody is connected.
Nothing here is stored, replayed or acknowledged — see
[Rooms](../explanation/rooms.md) and
[Add presence to an app](../how-to/add-presence.md).

```ts
ws.room<S, M>(name, opts?): Room<S, M>              // unanchored
ws.room<S, M>(schema, key, name, opts?): Room<S, M> // anchored to a row
```

Mirrored on the client itself for the default workspace. Anchoring makes
membership inherit that row's read permission.

### `Room<S, M>`

| Member                      | Behaviour                                                                                                                                                                              |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`, `sessionId`         | The room's name, and this tab's session — the key its own record lives under.                                                                                                          |
| `setPresence(state, opts?)` | Replaces this session's state wholly. Throttled; never queued offline but re-sent when the stream reopens. `opts.seq` records the workspace position it was authored against.          |
| `send(message)`             | Broadcasts to whoever is watching now. At most once. Rejects with a `RoomSendError` when the room is closed or dropped, and with the transport's error when the server is unreachable. |
| `peers()`                   | The roster including your own record, ordered by session id. Frozen, and reference-stable until something actually changes.                                                            |
| `subscribe(cb)`             | Roster changes. Returns an unsubscribe function.                                                                                                                                       |
| `onMessage(cb)`             | Broadcasts, including your own — filter on `RoomMessage.self`.                                                                                                                         |
| `onError(cb)`               | The room being dropped, and publish failures. Returns an unsubscribe function.                                                                                                         |
| `status()`                  | `joining` → `open`, or `dropped` (terminal) / `closed`.                                                                                                                                |
| `close()`                   | Withdraws this session's record and stops delivering. Idempotent.                                                                                                                      |

### `Peer<S>`

`sessionId`, `subject` (server-stamped; empty when unauthenticated), `state`
(`undefined` when the codec could not decode it), `seq`, and `self`. Peer
objects are immutable and keep their identity across roster changes, so
`React.memo` children only re-render for the peer that moved.

### Options

| Option       | Default       | Meaning                                                                                                                     |
| ------------ | ------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `codec`      | `jsonCodec()` | How state and messages are encoded. A cursor room passes `encodeAnchor()` bytes through instead of JSON.                    |
| `throttleMs` | 60            | Smallest gap between published states. The first of a quiet period goes out at once and the last of a burst always follows. |

`SyncClientOptions.presenceEnabled` (default true) turns the `Watch` stream off
while leaving publishing intact — for a page that writes presence and does not
render it, and to get a browser connection slot back. A record is normally kept
alive by whichever node holds its session's stream, so with no stream each open
room re-publishes its own last state on the interval `Publish` reports: one
unary call per room per interval in place of the connection.

### React

```ts
const { peers, room, loading, error } = useRoom<S, M>(name, opts?);
const peers = usePresence<S>(name, opts?);
```

Both take the anchored form too (`(schema, key, name, opts?)`), resolve the
workspace from `useWorkspaceScope`, and close the room on unmount or on an
identity change. `opts.onMessage` may be an inline closure — it is read through
a ref and does not reopen the room. Available as bare exports and on the
`createReactSync(entities)` bundle.

### Multi-tab

One `Watch` stream per account however many tabs and rooms are open, elected on
its own Web Lock — independently of the data stream's, so rooms work on a
polling-only client and the two leaders need not be the same tab. Publishing is
unary and happens directly from every tab. Where Web Locks or BroadcastChannel
are missing, every tab runs its own stream. See
[Multi-tab behaviour](../explanation/multi-tab.md).

---

## Quick mental model

- **Local-first:** every read/write hits the local store immediately; sync is in the
  background. Offline is the normal case.
- **Per-field LWW:** PATCH merges by field mask, "last to arrive at the server" wins.
  PUT replaces whole rows. Use `expectedVersion` (from `Row.version`) for CAS.
- **`db.<entity>` is the ergonomic surface;** schema-first methods are the escape
  hatch for simple-name clashes.
- **Watchers/queries are local & reactive;** `queryServer` is the only call that goes
  to the server for unsynced data and is _not_ merged with local state.
