# Naming rules

Entity names and row keys are part of the wire contract: they travel in
mutations, address rows in server storage, and are pasted into the client's
local key layout. Both implementations enforce the same rules — Go in
`engine/internal/naming`, TypeScript in `client/src/naming.ts` — so a write the
server would refuse never lands in the local store first.

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

## Workspace ids

Workspace ids are **not** covered by these rules: they are app-supplied strings
that may contain anything, and the client percent-encodes them where they appear
as a key segment. They are a hard partition key everywhere — see
[the convergence model](../explanation/convergence-model.md).
