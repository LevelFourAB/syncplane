# Naming rules

Entity names, workspace IDs, and row keys form part of the wire contract. They
appear in mutations, scope remote procedure calls (RPCs), address rows in server
storage, and structure key-value (KV) keys on the client. Both Go
(`engine/internal/naming`) and TypeScript (`client/src/naming.ts`) enforce these
rules.

## Entity names

An entity name identifies a schema entity. In Protocol Buffers, set the `name`
field in `(syncplane.v1.entity)`. In the TypeScript schema builder, call
`entity("name", ...)`.

The following table lists the syntax rules for entity names:

| Property      | Requirement                                                                       |
| ------------- | --------------------------------------------------------------------------------- |
| Character set | ASCII letter or `_`, followed by ASCII letters, digits, or `_` (`^[A-Za-z_]\w*$`) |
| Length        | 1 to 64 characters                                                                |

A schema cannot declare an entity whose name starts with `syncplane_`. The
engine keeps its own entities under that prefix, such as
`syncplane_field_history` and `syncplane_access_moves`. The match is
case-sensitive. The server refuses such a name at registration, and the
TypeScript schema builder's `entity()` throws on it.

Validation timing:

- **Server:** Checked during registration by `RegisterEntity` and `RegisterDescriptorSet`.
- **Client:** Checked during initialization by `new SyncClient({ entities })` and `entity()`.

## Reverse relation names

A reverse relation name identifies a [reverse relation](proto-options.md#reverse-relations) on its target entity. In Protocol Buffers, set `(syncplane.v1.field_behavior).reverse_relation` on the reference field. In the TypeScript schema builder, call `.reverseRelation("name")`. In a Go runtime schema, set `server.DynamicField.ReverseRelation`.

A reverse relation name follows the syntax rules for entity names: an ASCII letter or `_`, followed by ASCII letters, digits, or `_`, from 1 to 64 characters. A name that the schema does not set is `<plural(holder entity)>By<Field>` in camelCase, such as `issuesByAssignee`.

Validation timing:

- **Server:** Checked during registration by `RegisterEntity`, `RegisterDescriptorSet`, and `RegisterDynamicEntities`.
- **Client:** Checked during initialization by `new SyncClient({ entities })` and by `.reverseRelation()`.

## Permission and role names

Permission names and role names belong to the [access model](proto-options.md#access-model). A permission name appears in the `requires` of a rule in `(syncplane.v1.entity).access.allow`, in the `grants` of a role, and in a `MutatorAccess`. A role name appears in `(syncplane.v1.entity).grant.roles[].name`.

Both follow the syntax rules for entity names: an ASCII letter or `_`, followed by ASCII letters, digits, or `_`, from 1 to 64 characters.

Validation timing:

- **Server:** Checked during registration by `RegisterEntity`, `RegisterDescriptorSet`, and `RegisterDynamicEntities`.
- **Client:** Checked by `entity()` in the TypeScript schema builder.

## Key fields

The `key_fields` option specifies the fields whose values compose the row key,
in order. Each field must be a singular string, integer, boolean, or enum.

The following table lists the supported key field types and their string encodings:

| Field type                                                         | Key rendering     |
| ------------------------------------------------------------------ | ----------------- |
| `string`                                                           | Verbatim          |
| `int32`, `int64`, `uint32`, `uint64`, `sint*`, `fixed*`, `sfixed*` | Base-10           |
| `bool`                                                             | `true` or `false` |
| enum                                                               | Integer value     |

The following field types are not permitted: messages, repeated fields, maps,
`bytes`, `float`, and `double`.

Validation timing:

- Checked during registration on both the server and the client.
- The TypeScript schema builder rejects `bytes().key()` and `double().key()` at declaration time.

## Row keys

A row key is formed by joining the rendered values of `key_fields`:

- **Single key field:** The rendered value is used verbatim.
- **Multiple key fields:** Each value escapes `\` and `/`. The engine joins the escaped values with `/`. For example, the tuple `("a/b", "c")` renders as `a\/b/c`, and `("a", "b/c")` renders as `a/b\/c`.

The following table lists the constraints for row keys:

| Property      | Requirement                                                                                        |
| ------------- | -------------------------------------------------------------------------------------------------- |
| Character set | Valid UTF-8 with no unpaired surrogates; no ASCII control characters (`\u0000`–`\u001f`, `\u007f`) |
| Length        | 1 to 512 UTF-8 bytes (including escape characters)                                                 |
| Comparison    | Bytewise comparison without case folding or Unicode normalization                                  |

Validation and errors:

- **Client:** Checked when building mutations with `put`, `patch`, or `delete`.
- **Server:** Checked when applying mutations, including server-side writes through `EntityTx`.
- **Errors:** Over-the-wire validation failures return `invalid_argument`.

## Room names

A room topic consists of `(workspace_id, name)` and an optional row anchor. For
more information, see [Rooms](../explanation/rooms.md).

The following table lists the constraints for room names:

| Property      | Requirement                                                                                        |
| ------------- | -------------------------------------------------------------------------------------------------- |
| Character set | Valid UTF-8 with no unpaired surrogates; no ASCII control characters (`\u0000`–`\u001f`, `\u007f`) |
| Length        | 1 to 128 UTF-8 bytes                                                                               |
| Comparison    | Bytewise comparison without case folding or Unicode normalization                                  |

Examples of valid room names include `cursors`, `doc:42/cursors`, and `Board — Sprint 14`.

Validation and errors:

- **Client:** Checked when opening a room with `ws.room(name)`.
- **Server:** Checked at the RPC boundary.
- **Errors:** Validation failures return `invalid_argument`.

## Workspace ids

The `workspace_id` identifies a partition key. For more information, see
[the convergence model](../explanation/convergence-model.md).

The following table lists the constraints for workspace IDs:

| Property      | Requirement                                                                                           |
| ------------- | ----------------------------------------------------------------------------------------------------- |
| Character set | ASCII letter or digit, followed by letters, digits, `_`, `-`, or `:` (`^[A-Za-z0-9][A-Za-z0-9_:-]*$`) |
| Length        | 1 to 128 bytes                                                                                        |
| Comparison    | Bytewise comparison without case folding or Unicode normalization                                     |

Examples:

- **Valid:** `acme`, `acme-corp`, `user:xB12dL`, `org:acme:team:eng`, and UUIDs.
- **Invalid:** Characters such as spaces, `.`, `/`, and `*`.

Validation and errors:

- **Client:** Checked when opening a workspace with `new SyncClient({ workspaces })` or `addWorkspace`.
- **Server:** Checked at the RPC boundary. Validation failures return `invalid_argument` for the whole request.
- In-process server APIs (`Mutate`, connector paths, and effect paths) do not enforce workspace ID constraints.
