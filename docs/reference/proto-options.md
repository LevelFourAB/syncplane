# Proto options

Two custom options declare how a message participates in sync. They live in
`syncplane/v1/options.proto`, published as the buf module
`buf.build/levelfourab/syncplane`; see [Define entities in your own module](../how-to/define-entities.md) for getting that file into your codegen.

## `(syncplane.v1.entity)` — on a message

```proto
message Todo {
	option (syncplane.v1.entity) = {
		name: "todo"
		key_fields: [ "id" ]
	};
	string id = 1;
}
```

| Field           | Meaning                                                                                             |
| --------------- | --------------------------------------------------------------------------------------------------- |
| `name`          | Logical entity name used on the wire and in storage (e.g. `"todo"`).                                |
| `key_fields`    | Field names, in order, whose values form the row key. Singular scalars.                             |
| `index`         | Composite secondary indexes over this entity's fields. See [Secondary indexes](#secondary-indexes). |
| `delete_policy` | Who may DELETE rows of this entity. See [Delete policy](#delete-policy).                            |

The first two are constrained: the name must be an identifier, and a key field
must be a singular string, integer, bool or enum. See [Naming rules](naming.md)
for the full set and for what a derived row key may contain.

A message carrying this option is an **entity**. That is also what makes a
message-typed field elsewhere a _reference_ to it — reference detection is
annotation-free.

### Delete policy

`delete_policy` decides who may issue a `DELETE` against the entity. Unset (or
`DELETE_POLICY_ALLOW`) allows anyone the app's permission policy admits, which
is how every entity behaves that says nothing.

```proto
message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		delete_policy: DELETE_POLICY_TRUSTED_ONLY
	};
	string id = 1;
}
```

`DELETE_POLICY_TRUSTED_ONLY` refuses the DELETE to every caller the server
authorizes, with a `permission_denied` rejection. That includes the in-process
API — `Apply` and `Mutate` from an app handler are held to the same policy as a
sync client — and leaves only writes made under the `Trusted` bypass: a
connector, a migration, a cron job.

The engine checks it before the app's `Permissions.CanMutate` hook runs, on
every path that authorizes a write, so the rule is stated once in the schema
rather than restated in each deployment's policy.

It governs the DELETE _primitive_, not deletion as an outcome. A custom
mutator's rows are covered by the verdict on its invocation rather than
authorized one by one, so an app that wants a client-initiated delete of a
protected entity writes a mutator, puts the cascade handling and checks it needs
in the body, and authorizes the invocation by name:

```go
server.WithMutator("archiveProject", &appv1.ArchiveArgs{},
	func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
		// … whatever must happen first …
		return tx.Delete(&appv1.Project{}, args.(*appv1.ArchiveArgs).GetId())
	})
```

Reach for this on the rows a cascade hangs off. It removes the only
irreversible verb the wire has, rather than making its damage recoverable after
the fact.

## `(syncplane.v1.field_behavior)` — on a field

```proto
bytes body   = 2 [(syncplane.v1.field_behavior) = { text: true }];
bytes blob   = 3 [(syncplane.v1.field_behavior) = { opaque: true }];
repeated Member members = 4 [(syncplane.v1.field_behavior) = { key_field: [ "uid" ] }];
repeated Item items = 5 [(syncplane.v1.field_behavior) = { key_field: [ "id" ] order_field: "pos" }];
```

| Field         | Type       | Meaning                                                                                                                                                                                                                                                          |
| ------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `opaque`      | `bool`     | Replaced whole, never merged structurally. The escape hatch for CRDT blobs and for any list you want treated as one atomic value (order-sensitive, duplicate-bearing).                                                                                           |
| `key_field`   | `repeated` | List-of-message identity field(s); composite allowed. Empty, with an element that is a registered entity, reuses that entity's `key_fields`. Ignored for maps (keyed by map key) and scalar lists (keyed by value).                                              |
| `order_field` | `string`   | Names the position field on the element that orders this keyed list — a **movable list**, whose position the framework manages as a fractional index. Requires a keyed message list.                                                                             |
| `text`        | `bool`     | **Collaborative text.** The field holds an encoded convergent text document with Peritext-style marks and anchors. Requires a singular `bytes` field; implies `opaque`; the engine auto-installs the text merger. Incompatible with `key_field` / `order_field`. |
| `indexed`     | `bool`     | Maintain a single-component secondary index over this field; on a repeated scalar the row takes one entry per element. See [Secondary indexes](#secondary-indexes).                                                                                              |

## Secondary indexes

Two surfaces, one mechanism. Mark a field:

```proto
string state = 8 [(syncplane.v1.field_behavior) = { indexed: true }];
```

…or declare a composite on the entity:

```proto
option (syncplane.v1.entity) = {
	name: "issue"
	key_fields: [ "id" ]
	index: {
		name: "by_state_rank"
		key: [ { path: "state" }, { path: "rank" desc: true } ]
	}
};
```

A field mark desugars to a one-component index, so the engine only ever sees
one shape. Which to reach for:

- **Mark a field** when you filter or order by it. It says "this field is worth
  looking up by" without coupling the schema to a query.
- **Declare a composite** when a query filters on one field and orders by
  _another_. A sorted key-value store has no posting lists to intersect, so one
  marked field serving that query walks the sort order re-checking the predicate
  per candidate — correct, but unbounded in the bad case. A composite makes it
  exactly O(limit).

`IndexSpec.name` must be unique within the entity and must not start with `__`,
which is reserved: the engine derives one index per reference field for itself
(`__ref_<field>`), and a field mark becomes `__field_<field>`.

`IndexKey.path` is a dotted proto path. A path ending at a single-key reference
extends to that reference's key field, so an index and a query naming the same
reference agree. `desc` sorts that component descending; missing and `NaN`
values sort last whatever it says, because the index reproduces the query
ordering exactly and there the rank wins over the direction.

A path may run **through** a repeated field, or end at a repeated scalar
(`assignees.id`, `labels`). Such a component fans out: the row takes one
position per element, which is what makes `has`/`hasAny` and `any()` range reads
instead of scans. Three rules come with it:

- A row whose list is empty still takes one position, tagged missing, so a range
  that does not constrain the fan-out component still sees every row.
- Components of one index may only fan out over a **single** list. A second
  would need a cross product, which pairs values belonging to different
  elements — and a query meant to describe one element would match rows where no
  element does. Registration rejects it.
- A fan-out component supplies an ordering only where the equality prefix has
  already pinned it. `{ key: [{ path: "labels" }, { path: "rank" desc: true }] }`
  serves "labelled X, newest first" at O(limit); nothing can order **by** a
  repeated path, since a row has many values there.

**Not indexable:** a collaborative-text field (every keystroke would rewrite the
entry — full-text search is a different mechanism), a map, or a repeated message
field named on its own — index a scalar field of the elements instead. All are
rejected at registration.

Indexes are **invisible and additive**. Nothing in a request or response names
one, a query returns the same rows in the same order whether or not one served
it, and adding or removing one is never a breaking change — losing the stored
entries costs a rebuild, not correctness. The cost is on writes: every write to
the entity re-encodes and re-stores that row's position in **every** index it
has, bounded by `MaxIndexEntriesPerRow`.

## Merge behaviour by field kind

| Kind                 | On PATCH with the field in the mask        | On PUT                    |
| -------------------- | ------------------------------------------ | ------------------------- |
| Scalar, message, map | Last writer wins for that field            | Replaced                  |
| Keyed list           | Merged by element key                      | Replaced                  |
| `opaque`             | Replaced whole (or your registered merger) | Replaced                  |
| `text`               | Unioned with the stored document           | **Unioned**, not replaced |
| `oneof`              | Whole group replaced; mask names the group | Replaced                  |

The `text` row of that table is the one exception to "PUT replaces the whole
row" — see [Limits and trade-offs](../explanation/limits-and-trade-offs.md#put-replaces-the-whole-row).

## Oneofs

A oneof is one field. A mask names the **group** (`choice`), never a member
(`choice_text`), and the whole group is replaced by the writer's active case —
or cleared, when the writer has none. The client derives that mask for you:
`patch(schema, key, { choice: { case: 'choiceText', value: 'x' } })` and
`update(key, draft => …)` both emit the group path. Why the group and not the
member is in
[the convergence model](../explanation/convergence-model.md#a-oneof-is-one-field).

Reading is member-granular: a member is addressable on its own in queries,
orderings and `indexed` marks, where its presence means "this case is active".

```ts
const withValue = { where: (w) => w.choice.choiceText.eq('hit') }; // value of one case
const anyText = { where: (w) => w.choice.choiceText.isSet() }; // which case is active
```

### What a oneof cannot hold

Because the group is replaced whole, nothing below it is ever addressed on its
own — so anything that must be reliably present is refused when the server and
the client register the schema:

| Inside a oneof                                   | Why not                                                             |
| ------------------------------------------------ | ------------------------------------------------------------------- |
| An entity `key_field`                            | The row key would move when a sibling case won a write              |
| A list's `key_field` / `order_field`             | Elements would collide on the zero value once the case flipped      |
| A `text` field (directly or one level below)     | The group replaces whole, so the CRDT merger could never run        |
| A field with a registered merger                 | Same: the merger sits on a path no mask reaches                     |
| A reference (a field whose message is an entity) | The reference graph addresses a ref by a field that is always there |

The last one rules out polymorphic targets
(`oneof target { Project p = 1; User u = 2; }`). Accessor naming,
`include`/follow segments and the reverse push-down all key off a ref field
being in a fixed place; making them handle a sometimes-absent one is a design
question of its own rather than a missing branch.

## Custom mergers for opaque fields

`WithMerger` on the server plus a client `MergerSpec` install a merge
function for one opaque field. It **must be idempotent, commutative and
associative**: it runs on server apply, on optimistic overlay merge, and again on
every rebase replay, so a non-idempotent merger (a concat, say) corrupts data.

For collaborative prose do not write one — use `text: true`, whose built-in
union merger satisfies the contract by construction.

## A note on the option field numbers

The custom option field numbers (50100/50101) sit in the range reserved for use
within a single organization and are not registered. Two libraries claiming one
number is unrecoverable for an app depending on both — protobuf-go panics at
init with `extension number 50100 is already registered on message
google.protobuf.MessageOptions` — so claim numbers in protobuf's global
extension registry before publishing the protos. Moving them afterwards is a
breaking change for every adopter's generated code.
