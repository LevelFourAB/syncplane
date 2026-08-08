# Define schemas in TypeScript

The engine is descriptor-driven end to end — merge, query, CRUD, bootstrap and
subscribe all operate on `protoreflect` descriptors, not generated structs — so
an app's schema can arrive **at runtime** as a serialized
`google.protobuf.FileDescriptorSet` instead of compiled-in Go. Use this when you
would rather not run proto codegen, or when the app that owns the schema is the
Node backend rather than the sync server.

The TypeScript schema builder emits exactly those bytes:

```ts
import { schema } from 'syncplane';

const todo = schema.entity('todo', {
	id: schema.text().key(),
	title: schema.text(),
});

const bundle = schema.defineSchema(todo);
bundle.toBinary(); // a self-contained FileDescriptorSet the engine ingests
```

Register it with [a manager](host-multiple-apps.md#registering-a-schema), and
hand the same bundle to the client as `entities: bundle.descMessages()`.

## Full parity with the compiled tier

Not just scalars, but embedded sub-messages, typed references (to-one, to-many,
self, composite key), keyed and movable message lists, and scalar maps:

```ts
const project = schema.entity('project', {
	id: schema.text().key(),
	name: schema.text(),
});
const user = schema.entity('user', {
	id: schema.text().key(),
	name: schema.text(),
});

const issue = schema.entity('issue', {
	id: schema.text().key(),
	title: schema.text(),
	settings: schema.message({ color: schema.text(), size: schema.int32() }), // embedded
	project: schema.ref(() => project), // to-one reference
	assignee: schema.ref(() => user),
	reporter: schema.ref(() => user), // two refs → issuesByAssignee / issuesByReporter
	blockedBy: schema.refList(() => issue), // to-many SELF reference
	members: schema.keyedList(
		schema.message({ uid: schema.text(), name: schema.text() }),
		'uid',
	),
	items: schema.keyedList(
		schema.message({
			id: schema.text(),
			pos: schema.text(),
			label: schema.text(),
		}),
		'id',
	).order('pos'), // movable list (fractional `pos`)
	attrs: schema.map('string', schema.text()), // map<string,string>
});

const bundle = schema.defineSchema(project, user, issue);
```

## References

References take a **thunk** (`ref(() => target)`) rather than the target value:
the thunk defers resolution to `defineSchema` time, so an entity can reference
one declared later — or itself (`blockedBy` above) — without a definition-order
constraint. The whole schema is synthesized into **one** proto file, so mutual
and self references resolve by full name with no circular file-import graph.

Reference detection is **annotation-free**: a message-typed field whose target
message carries `(syncplane.v1.entity)` _is_ a reference — the same rule the
engine already applies. The stored row keeps only the target's key fields
(write-time projection); the live target is resolved on read, and the inverse
reverse relation is auto-named on the target.

The auto-name is `<plural(source)>` — or `<plural(source)>By<Field>` when one
source has several refs to the same target. It lands in the same namespace as
the target's own ref fields, since a follow segment, an `include` key and
`client.db.<entity>.refs` all resolve names against both halves at once. A
collision is therefore a schema error rather than a shadowing rule: `post` with
a `comments` ref field plus a `comment.post` back-ref (auto-named `comments` on
`post`) is rejected by the client registry and the engine alike. Rename one of
the two.

> Self-references hit a TypeScript inference cycle for the row type. Name the
> recursive field map and annotate the entity to break it — the same
> `type X = Infer<typeof x>` escape hatch you would use for "excessively deep"
> errors.

## Per-entity options

The third argument to `entity()` carries what is not a field. `indexes`
declares composite secondary indexes; `createPolicy`, `updatePolicy` and
`deletePolicy` decide who may write rows:

```ts
// Server-owned: the server records these rows, clients only read them.
const activity = schema.entity('activity', {
	id: schema.text().key(),
	what: schema.text(),
}, {
	createPolicy: 'trustedOnly',
	updatePolicy: 'trustedOnly',
	deletePolicy: 'trustedOnly',
});

// Append-only: a client may post a comment but not silently edit or remove one.
const comment = schema.entity('comment', {
	id: schema.text().key(),
	body: schema.text(),
}, { updatePolicy: 'trustedOnly', deletePolicy: 'trustedOnly' });
```

`'trustedOnly'` lowers to the matching `*_TRUSTED_ONLY` proto option, so a
runtime-registered entity is protected exactly as a compiled one is — that class
of write is left to trusted server-side writes and custom mutators.

The client refuses such a write at the call site rather than applying it
optimistically, so `comment.mutate.del(...)` throws. Creating an append-only row
takes `{ expectedVersion: 0n }` on the `put()`: an unguarded put could replace an
existing row, which the entity forbids. See
[Write policies](../reference/proto-options.md#delete-policy).

## The same power from Go

`server.DynamicField` (`Message`, `Ref`, `KeyField`/`OrderField`,
`MapKey`/`MapValue`) with `server.BuildDynamicFile` /
`RegisterDynamicEntities` builds a set of referencing entities into one file
just like the TS builder.

## What runtime registration guards

Because the engine is descriptor-driven, none of this needs runtime changes
beyond two registration-time guards: a re-registered message field must keep the
same message type (so a ref cannot be silently retargeted), and a key field must
be a singular scalar. Schema evolution rules are otherwise the same as for
compiled entities — see [Host multiple apps](host-multiple-apps.md#what-registration-guarantees).
