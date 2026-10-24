# Defining schemas in TypeScript

This guide shows you how to define schemas at runtime in TypeScript and export binary descriptor sets for the Syncplane engine and client. Use this approach when you want to avoid Protocol Buffers code generation, or when your Node backend owns the schema instead of the sync server.

The Syncplane engine operates on `protoreflect` descriptors for merge, query, CRUD, bootstrap, and subscribe operations rather than generated Go structs.

## Prerequisites

Before you begin, install the `syncplane` package in your TypeScript project:

```ts
import { schema } from 'syncplane';
```

## Defining entities and fields

1. Define each entity with `schema.entity()`. Specify its name and field map:

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
   	assignee: schema.ref(() => user), // user.refs.issuesByAssignee
   	reporter: schema.ref(() => user).reverseRelation('reportedIssues'), // user.refs.reportedIssues
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

   The schema builder supports the following field types:
   - **Key fields and scalars**: Mark primary keys with `.key()`, such as `schema.text().key()`.
   - **Embedded sub-messages**: Use `schema.message({ ... })` for nested structures.
   - **References**: Use a thunk (`ref(() => target)` or `refList(() => target)`). Thunks defer target resolution to bundle definition time. This avoids definition-order constraints and circular imports.
   - **Keyed lists**: Use `schema.keyedList(...)` with a unique key field identifier. Call `.order('fieldName')` to create a movable list.
   - **Scalar maps**: Use `schema.map('keyType', valueType)`.

   Reference detection does not require annotations. Syncplane treats a message field whose target carries `(syncplane.v1.entity)` as a reference. The stored row retains only the key fields of the target, and the live target resolves on read.

   Each reference adds a reverse relation to its target entity. The relation name is the value set by `.reverseRelation('name')`. If unset, the name defaults to the plural of the holder entity and the field name, such as `issuesByAssignee` for `issue.assignee`. Adding a reference never renames an existing reverse relation. A configured name must differ from the other reverse relations and reference fields of its target, or the client throws an error at construction. See [Reverse relations](../reference/proto-options.md#reverse-relations).

   **Note:** If you encounter a TypeScript inference cycle with self-referencing entities, name the recursive field map and annotate the entity type using `type X = Infer<typeof x>`.

2. Configure access rules and field guards.

   To decide who reads and writes entity rows, pass `access` in the options object as the third argument to `schema.entity()`. Each rule admits operations to callers that hold a permission, or to every admitted caller with `admitted: true`. An operation that no rule admits is open only to trusted server-side writes:

   ```ts
   // Server-owned: the server records these rows, clients only read them.
   const activity = schema.entity('activity', {
   	id: schema.text().key(),
   	what: schema.text(),
   }, {
   	access: { allow: [ { read: true, requires: 'view_activity' } ] },
   });

   // Append-only: a client may post a comment but not edit or remove one.
   const comment = schema.entity('comment', {
   	id: schema.text().key(),
   	body: schema.text(),
   }, {
   	access: {
   		allow: [
   			{ read: true, requires: 'view_comments' },
   			{ create: true, requires: 'post_comments' },
   		],
   	},
   });
   ```

   An entity without `access` remains open to every caller in the workspace. A role of a grant entity must grant each permission that a rule names. The server refuses to register these two entities alone. Add a grant entity whose roles grant `view_activity`, `view_comments`, and `post_comments`. See [Declare roles and permissions](declare-roles-and-permissions.md).

   At the call site, the client rejects a write that no rule admits. For example, `comment.mutate.del(...)` rejects with an error. A `put()` creates a comment, and the server refuses a `put()` of an existing comment. For more information, see [Access model](../reference/typescript-client.md#access-model).

   In a schema without `access`, reject these writes in the server's `CanMutate` hook. See [Reserve an operation for trusted writes](authentication-and-permissions.md#reserve-an-operation-for-trusted-writes).

   To restrict who writes specific fields while keeping the entity writable, add a `guard` to `access`. A guard whose only rule is the read rule keeps its fields from all client writes:

   ```ts
   const task = schema.entity('task', {
   	id: schema.text().key(),
   	title: schema.text(),
   	status: schema.text(),
   	priority: schema.int32(),
   }, {
   	access: {
   		allow: [
   			{ read: true, requires: 'view_tasks' },
   			{ write: true, requires: 'edit_tasks' },
   		],
   		guard: [
   			// Advanced by a server-side workflow; clients read it, never write it.
   			{ fields: [ 'status' ], allow: [ { read: true, admitted: true } ] },
   			// Only a caller with set_priority changes the priority.
   			{
   				fields: [ 'priority' ],
   				allow: [ { read: true, admitted: true }, {
   					write: true,
   					requires: 'set_priority',
   				} ],
   			},
   		],
   	},
   });
   ```

   A `patch` operation that masks `status` throws an error. A `put` operation succeeds and preserves the stored value of each guarded field that the caller cannot write. For more information, see [Field guards](../reference/proto-options.md#field-guards).

   A guard read rule narrower than `{ read: true, admitted: true }` hides its fields from some readers. A guard without a read rule hides its fields from every client. The client receives each hidden field cleared, and `Row.hidden` names it:

   ```ts
   guard: [ { fields: [ 'salary' ], allow: [ { read: true, write: true, requires: 'hr' } ] } ],
   ```

   For more information, see [Who reads a guarded field](../reference/proto-options.md#who-reads-a-guarded-field).

3. Create the schema bundle with `schema.defineSchema()`:

   ```ts
   const todo = schema.entity('todo', {
   	id: schema.text().key(),
   	title: schema.text(),
   });

   const bundle = schema.defineSchema(todo);
   ```

4. Export and register the schema:

   - Call `bundle.toBinary()` to export the serialized binary descriptor set. Register the resulting `google.protobuf.FileDescriptorSet` with [a manager](host-multiple-apps.md#steps).
   - Pass the client descriptors to the client configuration using `entities: bundle.descMessages()`.

## Confirming the result

To confirm that the schema is registered and active:

1. Verify that the manager accepts the binary descriptor set without errors during registration.
2. Verify that the client loads the entity descriptors from `bundle.descMessages()`.

If registration fails, check for the following conditions:

- A re-registered message field changed its message type. Re-registered fields must keep the same message type.
- A key field is not a singular scalar. Key fields must be singular scalar types.
- A field name collides with an auto-generated relation name on the same entity. Rename one of the conflicting fields.

For schema evolution rules, see [Host multiple apps](../reference/server.md#registration-guarantees).

## Related

- [Registration](../reference/server.md#registration)
