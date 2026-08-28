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

Validation timing:

- **Server:** Checked during registration by `RegisterEntity` and `RegisterDescriptorSet`.
- **Client:** Checked during initialization by `new SyncClient({ entities })` and `entity()`.

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
- **Multiple key fields:** Each value escapes `\` and `/`, and the escaped values are joined with `/`. For example, the tuple `("a/b", "c")` renders as `a\/b/c`, while `("a", "b/c")` renders as `a/b\/c`.

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

## Design rationale

This section documents the technical reasons behind the naming constraints.

### Entity name constraints

An entity name serves four purposes:

- A path segment in client KV storage keys (disallowing `/` prevents collisions between entity `a` with row `b/x` and entity `a/b` with row `x`).
- A token in the cross-tab change notification channel.
- A property name on the client database facade (`db.todo`).
- The source of synthesized Protocol Buffer message names in TypeScript schemas.

### Key field restrictions

Key fields reject complex and floating-point types for the following reasons:

- Messages, repeated fields, and maps have no text representation.
- `bytes`, `float`, and `double` have language-specific string representations. For example, Go renders `bytes` through `protoreflect` while TypeScript uses `Uint8Array`, and Go formats `1e7` as `1e+07` whereas JavaScript formats it as `10000000`.

### Row key constraints

- **Non-empty:** An unset key field derives `""`, which matches the entity prefix key in KV storage.
- **No ASCII control characters:** `\u001f` separates fields in cross-tab change tokens.
- **UTF-8 encoding:** Encoders substitute `\ufffd` for unpaired surrogates, which stores an incorrect key.
- **512-byte limit:** Keys are stored, scanned, and prefix-matched; bounding key length bounds index entry sizes.
- **Error handling:** Rejecting an invalid key with `invalid_argument` acts as an acknowledgement, allowing the client to drain its queue instead of retrying.

### Room name limits

Room names are limited to 128 bytes because the full topic (`workspace_id`, `name`, anchor key, and publisher subject) is transmitted in presence events alongside application state. Message buses running over PostgreSQL `LISTEN`/`NOTIFY` must fit the payload within a 7900-byte limit when state is base64-encoded.

### Workspace ID constraints

Workspace IDs use an allowlist to prevent delimiter collisions:

- `U+001F` separates fields in cross-tab change tokens, where `workspace_id` is the first field.
- `*` is the wildcard sentinel in the client watch layer for matching all workspaces.
- `/` introduces additional path segments in the client storage layout (`c/<entity>/<ws>/<key>`).
- `.` creates path traversal issues in admin UI URL paths (such as `..`).
- Requiring an alphanumeric leading character prevents IDs composed entirely of punctuation.
