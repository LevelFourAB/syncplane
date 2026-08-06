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

| Option                | Type                                           | Notes                                                                                                                                                                                      |
| --------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `entities`            | `E` (`readonly DescMessage[]`)                 | **Required.** All syncable entity descriptors. Pass as a `const` tuple so `db` types per entity.                                                                                           |
| `workspaceId`         | `string`                                       | Shorthand for `workspaces: [id], defaultWorkspace: id`. Give this **or** `workspaces`.                                                                                                     |
| `workspaces`          | `string[]`                                     | Workspaces to sync. Add and remove more at runtime.                                                                                                                                        |
| `defaultWorkspace`    | `string`                                       | What the client-level API (`client.mutate`, `client.db`, `client.get/scan/query/watch`, `transact`, undo) binds to. Defaults to the only configured workspace; **required** past one.      |
| `accountId`           | `string`                                       | Storage scope — one IndexedDB database, KV mutex, change channel and leadership lock per account. Defaults to the single workspace's id; **required** when starting with more than one.    |
| `url`                 | `string`                                       | Server base URL. Either `url` **or** `transport` required.                                                                                                                                 |
| `transport`           | `Transport` (`@connectrpc/connect`)            | Prebuilt Connect transport (tests / Node).                                                                                                                                                 |
| `wireFormat`          | `'binary' \| 'json'`                           | Encoding of the transport built from `url`. Default `'binary'`; `'json'` trades size and decode cost for traffic you can read in the network inspector. Ignored when `transport` is given. |
| `kvStore`             | `KVStore`                                      | Defaults to IndexedDB in browsers, in-memory elsewhere.                                                                                                                                    |
| `mutators`            | `Record<string, MutatorMirror<DescMessage>>`   | Best-effort local mirrors of server-side custom mutators.                                                                                                                                  |
| `mergers`             | `MergerSpec[]`                                 | Client mirrors of server merge hooks for opaque fields; divergence self-heals on next rebase.                                                                                              |
| `onMutationRejected`  | `(info: RejectionInfo) => void`                | Dead-letter callback (rejection or poison drop). Best-effort, **not durable**.                                                                                                             |
| `onStatusChange`      | `(status: SyncStatus) => void`                 | Same as `onStatusChange()` method.                                                                                                                                                         |
| `onSyncEvent`         | `(event: SyncEvent) => void`                   | Observability seam: push/pull/bootstrap/rebase/poke/stream/leadership events with durations and counts. No OpenTelemetry dependency — see [Telemetry](telemetry.md#the-typescript-client). |
| `pollIntervalMs`      | `number`                                       | Fallback poll; `0` disables. Default 60s.                                                                                                                                                  |
| `pokeEnabled`         | `boolean`                                      | Maintain poke stream. Default `true`.                                                                                                                                                      |
| `pushDebounceMs`      | `number`                                       | How long a local write waits before it is pushed, restarted by each further write, so a burst ships as one compacted push. Default 50ms; `0` pushes on every write.                        |
| `pushMaxDelayMs`      | `number`                                       | Ceiling on that wait, from the first write of a burst, so continuous writing still pushes regularly. Default 250ms; `0` removes the ceiling.                                               |
| `pullLimit`           | `number`                                       | Max row changes per pull page; `0` = server default.                                                                                                                                       |
| `poisonThreshold`     | `number`                                       | Deterministic push failures before dropping head mutation to dead-letter. Default 5; `<=0` disables.                                                                                       |
| `maxMutationsPerPush` | `number`                                       | Max batch per push RPC. Default 256.                                                                                                                                                       |
| `subscriptions`       | `SubscriptionSpec[]`                           | Partial sync. Empty/absent = full sync.                                                                                                                                                    |
| `bootstrapPriority`   | `readonly DescMessage[]`                       | Entities the first snapshot delivers first — what the first screen renders. A reordering, never a filter; only cold starts feel it.                                                        |
| `schemaRevision`      | `string`                                       | Opaque token matched against server's; drives skew handshake.                                                                                                                              |
| `onSchemaSkew`        | `(info: {client, server}) => SchemaSkewAction` | `'ignore' \| 'warn' \| 'block' \| 'resync'`. Default `'warn'` when `schemaRevision` set.                                                                                                   |
| `undo`                | `UndoOptions`                                  | Enables in-memory undo/redo.                                                                                                                                                               |

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
await client.scan(Todo):        Promise<Row<MessageShape<D>>[]>
await client.query(Todo, q, { include? }): Promise<Row<MessageShape<D>>[]>
await client.queryServer(Todo, q):         Promise<Row<MessageShape<D>>[]>  // server push-down; NOT merged with overlay

// db facade (identical semantics)
await client.db.todo.get(key)
await client.db.todo.getRow(key)
await client.db.todo.scan()
await client.db.todo.query(q, { include? })
await client.db.todo.queryServer(q)
```

### `Row<T>`

```ts
interface Row<T = unknown> {
	key: string;
	value: T;
	version: bigint; // server-confirmed version this row is based on; 0n if optimistic-only.
	// Pass as `expectedVersion` for compare-and-swap.
}
```

- `get` returns the decoded value only; `getRow` returns the `Row` (with `version`,
  needed for CAS). `query`/`scan` return `Row[]`.
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
  defaults to the keys present in `init`.
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

### Operator surface (by field type)

The proxy maps each field to operators by its value type. **Operator names are
reserved** — a proto field literally named `eq`/`has`/`isSet` is shadowed and
cannot be filtered or ordered on; registering such a schema logs a warning
naming the field. **Enums arrive as numbers**, so it is `t.status.eq(2)`, not
`t.status.eq(Status.DONE)`.

| Field type                                      | Operators                                                                    |
| ----------------------------------------------- | ---------------------------------------------------------------------------- |
| `string` (`StringOps`)                          | `eq`, `neq`, `in`, `startsWith`, `endsWith`, `contains` + order              |
| `number`/`bigint` (`NumericOps`)                | `eq`, `neq`, `in`, `gt`, `gte`, `lt`, `lte` + order                          |
| `boolean` (`BoolOps`)                           | `eq`, `neq` + order                                                          |
| `Uint8Array` & singular message (`PresenceOps`) | `isSet`, `isNotSet`                                                          |
| repeated scalar (`RepeatedScalarOps`)           | `has`, `hasAny`, `hasAll`, `isEmpty`                                         |
| repeated message (`RepeatedMessageOps`)         | `any(cb)`, `isEmpty`                                                         |
| singular message                                | drillable (`t.settings.color.eq(...)`) + `isSet`/`isNotSet`                  |
| oneof group (`OneofRef`)                        | one entry per case: the member's operators + `isSet`/`isNotSet`              |
| map (`MapOps`)                                  | `at(key)` → the value's operators + `isSet`/`isNotSet`; `any(cb)`; `isEmpty` |

Order: every orderable field exposes `.asc()` / `.desc()` → `OrderKey`.

Boolean combinators (top-level exports): `and(...conds)`, `or(...conds)`, `not(cond)`.

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
interface CompiledQuery { where?: Cond; orderBy?: OrderKey[]; limit?: number; offset?: number; }
```

`QueryRef`, `CompiledQuery`, `Query`, `Cond`, `OrderKey`, `Path` and `Scalar` are
exported as types. The converters that put a query on the wire are internal — the
client sends one for you, and the AST above is what you build.

---

## 5. Watchers (reactivity)

Scan-based: after any commit touching a (reachable) entity, the watcher re-scans and
fires with the full row list.

```ts
type WatchCallback<T> = (rows: Row<T>[]) => void;

const off = client.watch(Todo, (rows) => { ... });
const off = client.watchQuery(Todo, q, (rows) => { ... }, { include? });

// db facade
const off = client.db.todo.watch((rows) => { ... });
const off = client.db.todo.watchQuery(q, (rows) => { ... }, { include? });
```

- Returns a **synchronous unsubscribe** function.
- `watchQuery` suppresses callbacks when the result is unchanged (memoized on
  keys + bytes + versions).
- Callbacks must **not mutate** received rows (shared between same-entity watchers).
- Cross-tab: a commit in another tab re-fires this tab's watchers (via BroadcastChannel).

---

## 6. References & hydration

A reference field stores only the target's key fields; the live target is resolved
on read. All resolution is **orphan-tolerant** (missing target → `undefined`/empty).

### `include` (hydration)

`query`/`watchQuery` accept `{ include }`, an `IncludeSpec` tree. Each result row
gets a parallel `refs` object (`HydratedRow`):

```ts
interface IncludeSpec {
	[relation: string]: true | IncludeSpec;
}
interface HydratedRow<T> extends Row<T> {
	refs: Record<string, HydratedRow | HydratedRow[] | undefined>;
}

const rows = await client.db.task.query(
	{ where: (t) => t.done.eq(false) },
	{ include: { project: true, blockedBy: { project: true } } },
);
rows[0].refs.project; // HydratedRow | undefined (to-one)
```

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

---

## 9. Partial sync — subscriptions

```ts
import { subscription } from 'syncplane';

const specs = [
  subscription(Task, (t) => t.archived.eq(false), {
    include: { project: true },  // follow refs into scope
    depth: 2,                     // transitive follow rounds
  }),
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
}
function subscription<D>(
	schema,
	where?,
	follow?: { include?; depth? },
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

### Coverage

```ts
type CoverageBound = 'unsubscribed' | 'predicate' | 'window' | 'hydrating';
interface Coverage {
	complete: boolean;
	bounds: CoverageBound[];
	limit?: number; // present when a window is the only thing scoping the entity
}
```

Every result array carries one as a non-enumerable `coverage`. `hydrating` is the
transient bound: a cold client's first snapshot lands entity by entity, so an
entity that has not arrived reads as `[]` exactly like an empty one until this
says otherwise. It clears on its own and the readers re-fire.

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
costs O(result) rather than a scan — on the server, and on the client for
`query()`. For a query that filters on
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
  planner can express its predicate and ordering as a range. A cross-workspace
  query always scans.
- **`watchQuery()`** always scans, however well an index covers the same query
  passed to `query()`. Its snapshot is shared per (entity, workspace) rather
  than per query, and narrowing it to one query's matches would mean a snapshot
  per watcher.
- **Confirmed rows only.** The optimistic overlay is scanned whole and merged
  over the index range, which is O(pending) and keeps the answer identical to
  the merged view.

Set `debugVerifyIndexReads` (or `SYNC_DEBUG_VERIFY_TOUCHES=1`) to run both paths
on every query and throw if they disagree.

### Delete policy

The same options object carries `deletePolicy`, which decides who may delete
rows of the entity. It defaults to `'allow'`:

```ts
const project = schema.entity('project', {
	id: schema.text().key(),
	name: schema.text(),
}, { deletePolicy: 'trustedOnly' });
```

`'trustedOnly'` takes the DELETE primitive away from clients entirely: a
`del()` on such an entity comes back as a `permission_denied` rejection, which
the client acks and drops like any other. Deletion stays reachable through a
custom mutator, whose rows are covered by the verdict on the invocation, and
through trusted server-side writes. See
[Delete policy](proto-options.md#delete-policy) for the full semantics.

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
	useRow(key: string, opts?: UseRowOptions): UseRowResultFor<D>;
	useSuspenseRow(key: string, opts?: UseRowOptions): UseRowResultFor<D>;
	useMutate(opts?: UseWorkspaceOptions): EntityMutate<D>; // { put, patch, update, del }
	useText(key: string, field: string, opts?: UseWorkspaceOptions): UseTextResult;
}

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
- Every `useRow` on one (workspace, entity, include) shares a single subscription
  and a key index; a commit re-renders only the hooks whose own row moved.
- The `useSuspense*` variants wait for the first callback rather than reporting
  `loading`, and throw a failed subscription for an error boundary. Suspense is a
  separate hook, not an option, because it selects a fixed hook path.
- `UseWorkspaceOptions`: `{ workspaceId?: string; undo?: 'nearest' | 'workspace' }` —
  every hook accepts it. `UseWatchOptions`/`UseQueryOptions`/`UseRowOptions` add
  `{ include?: IncludeSpec }`.

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
- `subscription` builder + `SubscriptionSpec` type.
- Error codes: `CodeConflict`, `CodeInvalidArgument`, `CodePermissionDenied`,
  `CodePoisonDropped`.

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
