# Naming rules

Entity names, workspace ids and row keys are part of the wire contract: they
travel in mutations, scope every RPC, address rows in server storage, and are
pasted into the client's local key layout. Both implementations enforce the same
rules — Go in `engine/internal/naming`, TypeScript in `client/src/naming.ts` —
so a write the server would refuse never lands in the local store first.

## Entity names

The `name` in `(syncplane.v1.entity)` (and `entity("todo", …)` in the TypeScript
schema builder) must be an **identifier**:

| Rule                                                                |
| ------------------------------------------------------------------- |
| Non-empty                                                           |
| ASCII letter or `_`, then letters, digits or `_` (`^[A-Za-z_]\w*$`) |
| At most 64 characters                                               |

Checked at registration: `RegisterEntity` / `RegisterDescriptorSet` on the
server, `new SyncClient({ entities })` and `entity()` on the client.

The shape is narrow because an entity name is four things at once: a segment in
every client KV key (so no `/` — otherwise entity `a` row `b/x` and entity `a/b`
row `x` would occupy the same key), a token in the cross-tab change channel, a
property name on the client's db facade (`db.todo`), and — for schemas declared
in TypeScript — the source of a synthesized proto message name.

## Key fields

`key_fields` names the fields whose values form the row key, in order. Each must
be a **singular string, integer, bool or enum**.

| Kind                                                         | Renders as       |
| ------------------------------------------------------------ | ---------------- |
| `string`                                                     | Verbatim         |
| `int32` `int64` `uint32` `uint64` `sint*` `fixed*` `sfixed*` | Base-10          |
| `bool`                                                       | `true` / `false` |
| enum                                                         | The number       |

Refused: messages, repeated fields, maps, `bytes`, `float` and `double`. The
first three have no text form at all. The last three have one _per language_ —
Go renders `bytes` through protoreflect while the client holds a `Uint8Array`,
and `1e7` formats as `1e+07` in Go against `10000000` in JS — so a key built
from one would name a different row on each side.

Checked at registration on both sides, and the TypeScript builder refuses
`bytes().key()` / `double().key()` at the declaration.

## Row keys

The row key is the rendered key-field values joined:

- **One key field** → used verbatim, so the key reads as the id it is.
- **Several** → each part escapes `\` and `/`, then joined with `/`. This is
  what keeps the tuples `("a/b", "c")` and `("a", "b/c")` apart, as `a\/b/c`
  against `a/b\/c`.

The result must satisfy:

| Rule                                                      | Why                                                                                       |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Non-empty                                                 | An unset key field derives `""`, whose KV key is the entity prefix itself                 |
| No ASCII control characters (`\u0000`–`\u001f`, `\u007f`) | `\u001f` separates the fields of a cross-tab change token; keys should stay printable     |
| Encodable as UTF-8 (no unpaired surrogate)                | The encoder would silently substitute `\ufffd`, storing a key nobody derived              |
| At most 512 UTF-8 bytes, escaping included                | Keys are stored, scanned and prefix-matched; an unbounded key is an unbounded index entry |

Checked when a mutation is **built** on the client (`put` / `patch` / `delete`)
and when one is **applied** on the server, including trusted server-side writes
through `EntityTx`. A violation arriving over the wire is rejected with
`invalid_argument`; a rejection is an ack, so a client that minted a bad key
drains its queue instead of retrying forever.

Everything else is allowed. Keys may contain `/`, spaces and any printable
Unicode, and are compared bytewise — no case folding, no normalization, so `é`
written as one code point and as `e` plus a combining accent are different rows.

## Room names

A room topic is `(workspace_id, name)` plus an optional row anchor — see
[rooms](../explanation/rooms.md). Nothing declares a room ahead of time, so the
`name` half is a **character rule** rather than an identifier rule:

| Rule                                                      |
| --------------------------------------------------------- |
| Non-empty                                                 |
| No ASCII control characters (`\u0000`–`\u001f`, `\u007f`) |
| Encodable as UTF-8 (no unpaired surrogate)                |
| At most 128 UTF-8 bytes                                   |

So `cursors`, `doc:42/cursors` and `Board — Sprint 14` all work. Checked when a
room is opened on the client (`ws.room(name)`) and at the RPC boundary on the
server, where a violation is `invalid_argument`.

The cap is smaller than a row key's because the whole topic — workspace id,
name, anchor key, publisher subject — rides in every presence event alongside
the app's own state, and a bus over Postgres `LISTEN`/`NOTIFY` has to fit all of
it in one 7900-byte payload with the state base64-expanded. 128 bytes leaves
that budget to the state, which is the part an app cannot shorten.

Like row keys, names compare bytewise: `Notes` and `notes` are two rooms.

## Workspace ids

`workspace_id` is a hard partition key everywhere — see
[the convergence model](../explanation/convergence-model.md). Apps mint it
themselves, but it is not free-form:

| Rule                                                                                          |
| --------------------------------------------------------------------------------------------- |
| Non-empty                                                                                     |
| ASCII letter or digit, then letters, digits, `_`, `-` or `:` (`^[A-Za-z0-9][A-Za-z0-9_:-]*$`) |
| At most 128 bytes                                                                             |

So `acme`, `acme-corp`, `user:xB12dL`, `org:acme:team:eng` and a bare UUID all
work; a space, `.`, `/` or `*` does not. An app whose natural id does not fit —
an email address, a domain — slugifies or hashes it into one.

Checked on the client when a workspace is opened (`new SyncClient({ workspaces })`,
`addWorkspace`) and on the server at the RPC boundary, where a violation is
`invalid_argument` for the whole request rather than a per-workspace result: the
id names no partition to report against.

The set is an allowlist because the id is pasted into places that later parse it
back out, and each excluded character breaks one of them:

- `U+001F` separates the fields of the client's cross-tab change tokens, and the
  workspace is the first field — an id holding one shifts every later field and
  the notification wakes the wrong watchers.
- `*` is the sentinel the client's watch layer uses for "every workspace", so a
  workspace named `*` would share a scan with the cross-workspace watchers of
  the same entity.
- `/` forges an extra segment in the client's `c/<entity>/<ws>/<key>` layout.
- `.` reaches the admin UI as a URL path segment, where an id of `..` reads as a
  path traversal.

Restricting the first character keeps an id from being pure punctuation. Like
row keys, ids compare bytewise: no case folding and no normalization, so `Acme`
and `acme` are two different workspaces.

The in-process server API (`Mutate`, and the connector and effect paths that
reach it) does not enforce this — those ids come from application code, at the
same trust level as the schema it registers.
