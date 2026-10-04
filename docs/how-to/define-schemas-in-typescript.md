# Defining schemas in TypeScript

This guide shows you how to define schemas at runtime in TypeScript and export binary descriptor sets for the Syncplane engine and client. Use this approach when you want to avoid Protocol Buffers code generation, or when your Node backend owns the schema instead of the sync server.

The Syncplane engine is descriptor-driven. Merge, query, CRUD, bootstrap, and subscribe operations all operate on `protoreflect` descriptors rather than generated Go structs.

## Prerequisites

Before you begin, ensure that you have the `syncplane` package installed in your TypeScript project:

```ts
import { schema } from 'syncplane';
```

## Defining entities and fields

1. Define each entity with `schema.entity()`, specifying its name and field map:

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
   - **References**: Use a thunk (`ref(() => target)` or `refList(() => target)`). Thunks defer target resolution to bundle definition time, which avoids definition-order constraints and circular import issues.
   - **Keyed lists**: Use `schema.keyedList(...)` with a unique key field identifier. Call `.order('fieldName')` to create a movable list.
   - **Scalar maps**: Use `schema.map('keyType', valueType)`.

   Reference detection is annotation-free. A message-typed field whose target carries `(syncplane.v1.entity)` is treated as a reference. The stored row retains only the key fields of the target, and the live target resolves on read.

   Each reference adds a reverse relation to its target entity. Its name is the one `.reverseRelation('name')` sets, else the plural of the holder entity and the field name, such as `issuesByAssignee` for `issue.assignee`. Adding a reference never renames an existing reverse relation. A name that you set must differ from the other reverse relations and the reference fields of its target, or the client throws at construction. See [Reverse relations](../reference/proto-options.md#reverse-relations).

   **Note:** If you encounter a TypeScript inference cycle with self-referencing entities, name the recursive field map and annotate the entity type using `type X = Infer<typeof x>`.

2. Configure access rules and protected fields.

   To decide who reads and writes the rows of an entity, pass `access` in the options object, the third argument to `schema.entity()`. Each rule admits operations to the callers that hold a permission, or to every admitted caller with `admitted: true`. An operation that no rule admits is open only to trusted server-side writes:

   ```ts
   // Server-owned: the server records these rows, clients only read them.
   const activity = schema.entity('activity', {
   	id: schema.text().key(),
   	what: schema.text(),
   }, {
   	access: { allow: [ { read: true, permission: 'view_activity' } ] },
   });

   // Append-only: a client may post a comment but not edit or remove one.
   const comment = schema.entity('comment', {
   	id: schema.text().key(),
   	body: schema.text(),
   }, {
   	access: {
   		allow: [
   			{ read: true, permission: 'view_comments' },
   			{ create: true, permission: 'post_comments' },
   		],
   	},
   });
   ```

   An entity without `access` stays open to every caller that enters the workspace, and a role of a grant entity must grant each permission that a rule names. See [Declare roles and permissions](declare-roles-and-permissions.md).

   At the call site, the client rejects a write that no rule can admit. For example, `comment.mutate.del(...)` rejects with an error. A `put()` creates a comment, and the server refuses a `put()` of a comment that exists. For more information, see [Access model](../reference/typescript-client.md#access-model).

   In a schema without `access`, refuse such writes in the server's `CanMutate` hook. See [Reserve an operation for trusted writes](authentication-and-permissions.md#reserve-an-operation-for-trusted-writes).

   To restrict specific fields while keeping the entity writable, add `.protected()` to the field definition:

   ```ts
   const task = schema.entity('task', {
   	id: schema.text().key(),
   	title: schema.text(),
   	// Advanced by a server-side workflow; clients read it, never write it.
   	status: schema.text().protected(),
   });
   ```

   A `patch` operation that masks a protected field throws an error. A `put` operation succeeds and preserves the existing field value. For more information, see [Protected fields](../reference/proto-options.md#protected-fields).

3. Create the schema bundle with `schema.defineSchema()`:

   ```ts
   const todo = schema.entity('todo', {
   	id: schema.text().key(),
   	title: schema.text(),
   });

   const bundle = schema.defineSchema(todo);
   ```

4. Export and register the schema.

   - Export the serialized binary descriptor set by calling `bundle.toBinary()`. Register the resulting `google.protobuf.FileDescriptorSet` with [a manager](host-multiple-apps.md#steps).
   - Pass the client descriptors to the client configuration using `entities: bundle.descMessages()`.

## Confirming the result

To confirm that the schema is registered and active:

1. Verify that the manager accepts the binary descriptor set without errors during registration.
2. Verify that the client loads the entity descriptors from `bundle.descMessages()`.

If registration fails, check for the following conditions:

- A re-registered message field changed its message type. Re-registered fields must maintain the same message type.
- A key field is not a singular scalar. Key fields must be singular scalar types.
- A field name collides with an auto-generated relation name on the same entity. Rename one of the conflicting fields.

For schema evolution rules, see [Host multiple apps](../reference/server.md#registration-guarantees).

## Related

- [Registration](../reference/server.md#registration)
