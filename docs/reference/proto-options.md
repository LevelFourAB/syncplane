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
| `create_policy` | Who may create rows of this entity. See [Write policies](#delete-policy).                           |
| `update_policy` | Who may modify existing rows of this entity. See [Write policies](#delete-policy).                  |
| `delete_policy` | Who may DELETE rows of this entity. See [Write policies](#delete-policy).                           |
| `server_only`   | The entity's rows never sync to a client. See [Server-only entities](#server-only-entities).        |
| `history`       | Record an append-only history of field changes. See [Field history](#field-history).                |

The first two are constrained: the name must be an identifier, and a key field
must be a singular string, integer, bool or enum. See [Naming rules](naming.md)
for the full set and for what a derived row key may contain.

A message carrying this option is an **entity**. That is also what makes a
message-typed field elsewhere a _reference_ to it — reference detection is
annotation-free.

<a id="delete-policy"></a>

### Write policies

Three options decide who may write an entity's rows: `create_policy`,
`update_policy` and `delete_policy`. Each is unset by default, which allows
anyone the app's permission policy admits — how every entity behaves that says
nothing.

```proto
// Server-owned: the server records these rows, clients only read them.
message Activity {
	option (syncplane.v1.entity) = {
		name: "activity"
		key_fields: [ "id" ]
		create_policy: CREATE_POLICY_TRUSTED_ONLY
		update_policy: UPDATE_POLICY_TRUSTED_ONLY
		delete_policy: DELETE_POLICY_TRUSTED_ONLY
	};
	string id = 1;
}

// Append-only: a client may post a comment but not silently edit or remove one.
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		update_policy: UPDATE_POLICY_TRUSTED_ONLY
		delete_policy: DELETE_POLICY_TRUSTED_ONLY
	};
	string id = 1;
}
```

`*_TRUSTED_ONLY` refuses that class of write to every caller the server
authorizes, with a `permission_denied` rejection. That includes the in-process
API — `Apply` and `Mutate` from an app handler are held to the same policy as a
sync client — and leaves only writes made under the `Trusted` bypass: a
connector, a migration, a cron job.

The engine checks these before the app's `Permissions.CanMutate` hook runs, on
every path that authorizes a write, so the rule is stated once in the schema
rather than restated in each deployment's policy.

#### Which policy a write must satisfy

The check runs before the write transaction opens and never reads the row, so
it classifies from the write alone. `expected_version` is what makes that
possible: an absent or tombstoned row reads as version 0, so `expected_version:
0` means "apply only if the row does not exist".

| Write                                    | Must satisfy                                 |
| ---------------------------------------- | -------------------------------------------- |
| `PATCH`                                  | `update_policy` — a PATCH never upserts      |
| `DELETE`                                 | `delete_policy`                              |
| `PUT` with `expected_version` 0          | `create_policy` — provably a create          |
| `PUT` with a non-zero `expected_version` | `update_policy` — provably an update         |
| `PUT` with no `expected_version`         | **both** `create_policy` and `update_policy` |

An unguarded PUT is create-or-update and cannot be told apart, so it is
permitted only when it would be permitted either way. A client writing to an
entity that restricts updates must therefore assert `expected_version: 0` to say
it means to create — from Go, `server.WithExpectedVersion(0)` on the `Put`.

The TypeScript client applies the same table locally and throws at the call
site, so a write that can never land never enters the pending queue.

#### What they do not cover

They range over caller class — trusted bypass versus any authorized caller — not
over rows. "Only edit your own comment" is not expressible here; it belongs in
`CanMutate` when ownership is derivable from the key, or in a mutator body when
it needs the stored row.

They also range over whole rows, not fields. To keep one field of an otherwise
writable row out of a client's reach, mark the field instead — see
[Protected fields](#protected-fields).

They govern the write _primitives_, not the outcome. A custom mutator's rows are
covered by the verdict on its invocation rather than authorized one by one, so
an app that wants a client-initiated delete or edit of a protected entity writes
a mutator, puts the cascade handling and checks it needs in the body, and
authorizes the invocation by name:

```go
server.WithMutator("archiveProject", &appv1.ArchiveArgs{},
	func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
		// … whatever must happen first …
		return tx.Delete(&appv1.Project{}, args.(*appv1.ArchiveArgs).GetId())
	})
```

Two more limits are worth knowing. A batch applies in order inside one
savepoint, so `DELETE(k)` followed by `PUT(k, expected_version: 0)` passes as a
create and lands as a replace — `update_policy` constrains in-place edits, and
closing that loop takes `delete_policy` alongside it. And collaborative-text
fields are edited by PATCH, so an entity carrying one needs updates allowed for
clients to edit its text at all.

Reach for `delete_policy` on the rows a cascade hangs off: it removes the only
irreversible verb the wire has, rather than making its damage recoverable after
the fact.

### Server-only entities

`server_only: true` keeps an entity's rows on the server. They are never
bootstrapped, pulled or streamed; a subscription naming the entity is refused as
`invalid_argument`, and a client CRUD targeting it is rejected. The TypeScript
client reads the option off the schema it registers, so a subscription set naming
one throws where it is built rather than reaching that server error.

```proto
message AuditEvent {
	option (syncplane.v1.entity) = {
		name: "audit_event"
		key_fields: [ "id" ]
		server_only: true
	};
	string id = 1;
}
```

Server-side reads are untouched. The `Query` RPC (`queryServer` in the
TypeScript client), `ViewTx` and mutators all still reach the entity — it is
**queryable, not syncable**.

Subscriptions are not a substitute for this. An empty subscription list means
full sync, and full sync is _absorbing_ across the tabs sharing one local store:
one tab that never calls `setSubscriptions` pulls the entity into shared
IndexedDB and every sibling tab inherits it. Only a property of the schema holds
an entity back from all of them.

### Field history

`history: {}` records an append-only log of what a row's marked fields held
**before** each editing session that changed them.

```proto
message Issue {
	option (syncplane.v1.entity) = {
		name: "issue"
		key_fields: [ "id" ]
		history: {}
	};
	string id = 1;
	string title = 2 [(syncplane.v1.field_behavior) = { history: true }];
	string description = 3 [(syncplane.v1.field_behavior) = { history: true }];
	int64 sort_key = 4; // not marked, so not recorded
}
```

Every recorded field is marked; there is no capture-everything default. An
entity that opts in and marks none is **rejected at registration**.

Capture happens on the server's write path, so every write is covered whatever
made it: a client push, a mutator, a connector, a trusted write.

Records land in one entity, `syncplane.v1.FieldHistory` (wire name
`syncplane_field_history`), which the engine registers automatically the first
time some entity opts in. It is `server_only`, so none of it syncs; read it with
`queryServer`. See [Restore a field to a previous value](../how-to/restore-a-field.md).

| `FieldHistory` field            | Meaning                                                                                                                                                                                                                                                               |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `source_entity`, `source_key`   | The row that changed.                                                                                                                                                                                                                                                 |
| `seq`                           | The workspace sequence the before-state precedes — the sequence of the write that opened the record.                                                                                                                                                                  |
| `through_seq`, `writes`         | The last sequence folded into the record, and how many writes folded in (at least 1). The record covers the span `seq`…`through_seq`; spans of one row can overlap across nodes.                                                                                      |
| `row_created`                   | **The row did not exist when the session began**, so `mask` and `prior` are empty and there is nothing to restore. Read this rather than an empty `mask`: a history filter can empty a mask too.                                                                      |
| `mask`                          | The fields the session changed, as dotted proto paths.                                                                                                                                                                                                                |
| `prior`                         | The source entity, serialized, carrying its key fields plus the masked fields as they stood. A text field holds a **plain-string value** rather than a document; restoring resolves it into edits, and reading it for display takes `plainTextOf` / `text.PlainText`. |
| `actor_subject`, `actor_system` | Who wrote (see `storage.Actor`): the authenticated caller's subject, the label a trusted write gave itself, or both. One session belongs to one actor. Empty on both means the write was unattributed.                                                                |
| `recorded_at`                   | Server wall clock when the record opened, for display. Not an ordering; only `seq` orders writes.                                                                                                                                                                     |

Four rules govern what a record ends up naming:

- **Changes are recorded, not writes.** The server compares the stored row with
  the one replacing it and keeps only the fields that moved; a write that stores
  what was already there produces no record at all.
- **Sessions are recorded, not keystrokes.** A run of writes to one row by one
  actor folds into the record the first of them opened: the record keeps saying
  what the fields held before the run began, its mask widens to the union of what
  the run touched, and `through_seq`/`writes` grow instead of the log. No other
  record of that session covers the states in between. The window is
  [`WithHistorySession`](server.md#field-history), five minutes by default, and
  runs from when the record opened rather than from its last write.
- A **PATCH** starts from its effective mask (text paths a write option added
  included), a **PUT** from the whole capture set, and the comparison narrows
  either. Narrowing is per top-level field, the granularity per-field LWW
  resolves at, so two masked paths under one field stand or fall together. A
  oneof counts as one field and is named by its group.
- **Key fields are never in the mask**, however they are marked. A PATCH may not
  change a key field, so a restore built from a mask naming one could never be
  applied. `prior` still carries them, so a decoded record always says which row
  it belongs to.

Why the log is scoped this way, and what it costs, is in
[Limits and trade-offs](../explanation/limits-and-trade-offs.md#field-history-records-editing-sessions-not-keystrokes).

`DELETE` is not captured, and it **purges** the row's records — nothing can
restore into a tombstone, and a row re-created under the same key must not
inherit the old one's log. A record's lifetime is therefore bounded by its row as
well as by the clock, and under
`WithHistoryRetention(server.HistoryForever)` the row is the only bound. This
tier restores fields, not rows: making a removal recoverable and attributable
means modelling it as a field — see
[Recover a deleted row](../how-to/recover-a-deleted-row.md).

Removing `history: {}` later stops capture but does **not** remove what was
already captured. The engine keeps purging those records on `DELETE` and expiring
them under retention, for as long as some entity in the schema still opts in.

Records are read under **the source row's** permissions, not their own — see
[Field history](server.md#field-history).

Retention and an admission hook are server options — see
[`WithHistoryRetention` and `WithHistoryFilter`](server.md#field-history).

## `(syncplane.v1.field_behavior)` — on a field

```proto
bytes body   = 2 [(syncplane.v1.field_behavior) = { text: true }];
bytes blob   = 3 [(syncplane.v1.field_behavior) = { opaque: true }];
repeated Member members = 4 [(syncplane.v1.field_behavior) = { key_field: [ "uid" ] }];
repeated Item items = 5 [(syncplane.v1.field_behavior) = { key_field: [ "id" ] order_field: "pos" }];
```

| Field          | Type       | Meaning                                                                                                                                                                                                                                                                                                                       |
| -------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `opaque`       | `bool`     | Replaced whole, never merged structurally. The escape hatch for CRDT blobs and for any list you want treated as one atomic value (order-sensitive, duplicate-bearing).                                                                                                                                                        |
| `key_field`    | `repeated` | List-of-message identity field(s); composite allowed. Empty, with an element that is a registered entity, reuses that entity's `key_fields`. Ignored for maps (keyed by map key) and scalar lists (keyed by value).                                                                                                           |
| `order_field`  | `string`   | Names the position field on the element that orders this keyed list — a **movable list**, whose position the framework manages as a fractional index. Requires a keyed message list.                                                                                                                                          |
| `text`         | `bool`     | **Collaborative text.** The field holds an encoded convergent text document with Peritext-style marks and anchors. Requires a singular `bytes` field; implies `opaque`; the engine auto-installs the text merger. Incompatible with `key_field` / `order_field`.                                                              |
| `indexed`      | `bool`     | Maintain a single-component secondary index over this field; on a repeated scalar the row takes one entry per element. See [Secondary indexes](#secondary-indexes).                                                                                                                                                           |
| `history`      | `bool`     | Record this field in the entity's history. Capture is **explicit** — only marked fields are recorded, and an entity that opts in and marks none is rejected at registration. A oneof is captured as a group, so marking any member marks it. Ignored on an entity that has not opted in. See [Field history](#field-history). |
| `write_policy` | `enum`     | `FIELD_WRITE_POLICY_TRUSTED_ONLY` puts the field out of a client's reach while the rest of the row stays writable. Top-level fields only; a oneof is protected as a group; a key field may not be marked. See [Protected fields](#protected-fields).                                                                          |

## Protected fields

`create_policy`, `update_policy` and `delete_policy` speak for a whole row.
`write_policy` speaks for one field, which is what a status a workflow advances,
a counter an effect maintains or an owner stamped at creation needs:

```proto
message Task {
	option (syncplane.v1.entity) = { name: "task" key_fields: [ "id" ] };

	string id = 1;
	string title = 2;
	// Advanced by a server-side workflow; a client may read it, never write it.
	string status = 3
		[(syncplane.v1.field_behavior) = { write_policy: FIELD_WRITE_POLICY_TRUSTED_ONLY }];
}
```

The two write primitives are governed differently, because they say different
things:

| Write                                | Outcome                                                                   |
| ------------------------------------ | ------------------------------------------------------------------------- |
| `PATCH` whose mask reaches the field | Rejected with `permission_denied`                                         |
| `PUT`                                | Applied; the field keeps its stored value, or its zero value on a new row |
| `DELETE`                             | Unaffected — a protected field guards a value, not the row                |

A PATCH names its fields, so a mask reaching a protected one asks outright to do
the forbidden thing. A PUT names none: it carries the whole row because that is
the only shape it has, so refusing it would reject an honest read-modify-write
whenever the client's copy of a field it never meant to touch had gone stale.
Substituting the stored value lets the rest of the write land.

A mask is matched by its **first segment**, so protecting `audit` also refuses
`audit.size`, and one leaf of a message a client may otherwise replace cannot be
singled out. A oneof is protected as a group — the only unit a mask names it in
— so marking any member marks all of them. A key field may not be marked and is
rejected at registration: a PATCH can never move one, and a PUT that changed one
would address a different row.

A protected collaborative-text field is not editable by clients at all: edits
ship as PATCHes, and a PUT takes the stored document verbatim rather than
unioning the client's into it.

The TypeScript client reads the same marks and throws at the call site, so a
`patch()` naming a protected field never enters the pending queue and a `put()`
substitutes the local row's value into the optimistic overlay.

For a rule that depends on **who** is writing — "the owner may reassign, a viewer
may not" — a schema enum is the wrong surface; implement the server's
`FieldPermissions` hook, which adds fields to this set for one caller and can
never take one away. See
[Authentication and permissions](../how-to/authentication-and-permissions.md#protect-a-field-per-caller).

Like the entity policies, this governs the write primitives rather than
modification as an outcome. A custom mutator's rows are covered by the verdict on
its invocation, so an app that wants a client-initiated change to a protected
field writes a mutator, checks what it needs in the body, and authorizes the
invocation by name.

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

A path may also fan out over a **map**, written `*` — see
[Maps and user-defined fields](#maps-and-user-defined-fields) below.

**Not indexable:** a collaborative-text field (every keystroke would rewrite the
entry — searching text is what [full-text search](#full-text-search) is for), or
a repeated message field named on its own — index a scalar field of the elements
instead. A map is not indexable through a field mark either; it needs the `*`
syntax, which says which entries and which leaf. All are rejected at
registration.

Indexes are **invisible and additive**. Nothing in a request or response names
one, a query returns the same rows in the same order whether or not one served
it, and adding or removing one is never a breaking change — losing the stored
entries costs a rebuild, not correctness. The cost is on writes: every write to
the entity re-encodes and re-stores that row's position in **every** index it
has, bounded by `MaxIndexEntriesPerRow`.

## Full-text search

Two surfaces, like indexes: a field mark for the simple case, an entity block
for importance order and analyzer options. An entity may use either or both.

```proto
message Article {
	option (syncplane.v1.entity) = {
		name: "article"
		key_fields: [ "id" ]
		search: {
			field: [ { path: "summary" }, { path: "body" } ]
			keep_diacritics: false
		}
	};

	string id = 1;
	string title = 2 [(syncplane.v1.field_behavior) = { searchable: true }];
	string summary = 3;
	bytes body = 4 [(syncplane.v1.field_behavior) = { text: true }];
}
```

| Option                      | Meaning                                                                                                          |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `field_behavior.searchable` | Include this field, ahead of the `search` block's fields. Marked fields count as most important, in field order. |
| `search.field[].path`       | Dotted proto path to a singular string or collaborative-text field. Declaration order is importance order.       |
| `search.keep_diacritics`    | Keep accents significant: `café` and `cafe` become distinct terms. Off by default, which folds them together.    |

A `match` condition reads the declared fields through the entity's analyzer —
lowercased, accent-folded unless kept, segmented with CJK bigrams — and
`relevance` ranks by them, earlier fields first. A collaborative-text field is
searched as its visible plain string.

**Rejected at registration:** a path to a repeated field, a number, a map or a
message; a `searchable` mark on any of those; a `search` block naming no fields
with none marked either. A path may appear once across both surfaces.

Declaring search also declares a term index over the entity, maintained on
every write and backfilled in the background like any
[secondary index](#secondary-indexes); a row with more than 256 distinct terms
across its searchable fields stores an overflow marker instead of postings and
is considered by every search. Matching, ranking and the analyzer are specified
in [the search model](../explanation/search.md); the query surface is under
[Search data](../how-to/search.md).

## Merge behaviour by field kind

| Kind            | On PATCH with the field in the mask        | On PUT                    |
| --------------- | ------------------------------------------ | ------------------------- |
| Scalar, message | Last writer wins for that field            | Replaced                  |
| Map             | Merged by map key; a mask may name one key | Replaced                  |
| Keyed list      | Merged by element key                      | Replaced                  |
| `opaque`        | Replaced whole (or your registered merger) | Replaced                  |
| `text`          | Unioned with the stored document           | **Unioned**, not replaced |
| `oneof`         | Whole group replaced; mask names the group | Replaced                  |

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

That rule is load-bearing beyond oneofs themselves: it is what lets a map of
user-defined fields hold an **app-defined** value type instead of an engine one.
See below.

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

## Maps and user-defined fields

A `map` is merged per entry by map key, and every entry is addressable — in a
field mask, in a query, in an ordering. That is what an issue tracker's
user-defined ("custom") fields are made of:

```proto
message Issue {
	option (syncplane.v1.entity) = {
		name: "issue"
		key_fields: [ "id" ]
		index: { name: "cf_number" key: [ { path: "custom.*.number" } ] }
	};

	string id = 1;
	map<string, FieldValue> custom = 2;
}

// The value type is YOURS. The engine never interprets it.
message FieldValue {
	oneof kind {
		string text = 1;
		double number = 2;
		bool checked = 3;
		// Multi-select needs no engine concept: wrap a repeated scalar, and
		// `custom.<key>.labels.values` is an ordinary repeated leaf.
		StringList labels = 4;
	}
}
```

### Addressing one entry

**The path segment after a map field is the entry KEY**, not a field name — the
same convention a field-mask path already uses, so there is one rule rather than
two:

```
custom.priority.number      the `number` variant of the entry keyed "priority"
custom.priority             the whole entry (isSet / isNotSet)
custom                      the whole map (isEmpty)
```

A key that contains a dot is escaped with a backslash where a path is written as
one string — `custom.due\.date.text` — which is only needed in the Go builders
and in field masks. On the wire a path is already a list of segments, so a key
travels verbatim.

A query names the VARIANT it means. Because a oneof member has explicit
presence, an entry holding a different variant reads as **not found**:
the condition is false and the ordering ranks it last, exactly as for any unset
field. That is why the engine needs no canonical value type, no cross-variant
sort order, and no rule for comparing a number against an attachment — no path
ever asks it to.

The one consequence to accept: **there is no ordering by "whatever the value
is"** across variants. `order by custom.priority` is not a thing;
`order by custom.priority.number` is. Since a custom field has one declared type
in your own field-definition table, that is a non-loss in practice — but it does
follow directly from choosing an app-defined value type.

Comparing a whole map, or a whole entry whose value is a message, is rejected at
compile time with an error naming the key form. `isSet` on a map is not offered:
a map has implicit presence, so it is constitutionally true. `isEmpty` is what
asks whether it holds anything.

### Searching across keys: `any()`

The mirror question — "**which** custom field holds X", rather than "does THIS
one" — is an `any()` over the map. Its element is one **entry**, whose two sides
the sub-condition addresses as `key` and `value`:

```go
query.Any("custom", query.Eq("value.number", 5))            // which field holds 5
query.Any("custom", query.And(
	query.StartsWith("key", "cf_"), query.Eq("value.checked", true)))
query.Any("custom", query.In("key", "priority", "severity"))  // any of these set
```

```ts
w.custom.any((e) => e.value.kind.number.eq(5));
w.custom.any((e) =>
	and(e.key.startsWith('cf_'), e.value.kind.checked.eq(true))
);
```

The entry scope is explicit for the same reason it is on a repeated message
field: it is what lets two conditions be said to describe the **same** entry.
`any(and(key == "priority", value.number == 5))` is "the field named priority
holds 5"; `and(any(key == "priority"), any(value.number == 5))` is the weaker
"priority is set AND something holds 5".

Two things follow, and both are worth knowing before you reach for it:

- **Comparing the key as a value** costs no escaping — an entry key containing a
  dot needs none here — and gives `in` and `startsWith` over keys for free.
- **There is no projection.** The answer is a predicate: some entry matched. It
  never says _which_ one. An app that needs the field's name reads it off the
  row's map after the query returns.

### Indexing every entry: `*`

A `*` segment in an `IndexKey.path` means **fan out over every entry of this
map**, and the segment after it — `key` or `value` — says which side of the
entry the component stores. **One declared path is one component**, so an index
states its own layout:

```proto
// One custom field, addressed by key: equality, ranges and an ordering within it.
index {
	name: "cf_number"
	key: [ { path: "custom.*.key" }, { path: "custom.*.value.number" } ]
}
// The same, with a second sort key.
index {
	name: "cf_num_recent"
	key: [
		{ path: "custom.*.key" },
		{ path: "custom.*.value.number" },
		{ path: "updated_at" desc: true }
	]
}
// Which custom field holds X: equality only, cross-key or named-key.
index {
	name: "cf_by_number"
	key: [ { path: "custom.*.value.number" }, { path: "custom.*.key" } ]
}
// Multi-select, one entry per (key, element).
index {
	name: "cf_labels"
	key: [ { path: "custom.*.key" }, { path: "custom.*.value.labels.values" } ]
}
```

The two layouts are complementary, and the choice between them is the one real
decision here:

| Layout         | Serves                                                                 |
| -------------- | ---------------------------------------------------------------------- |
| `[key, value]` | equality, ranges **and** ordering — within one named field             |
| `[value, key]` | equality only — cross-key _or_ named-key (pin the value, then the key) |

An app that only does equality probes declares **one** index, `[value, key]`, and
gets both axes from it. Declaring both layouts doubles the entries a row
occupies; see [limits and trade-offs](../explanation/limits-and-trade-offs.md)
for what that costs and how to measure it.

The selector is required rather than defaulted because a bare trailing `*` would
be genuinely ambiguous: for `map<string, string> attrs`, `attrs.*` could mean
either side of the entry. `key` and `value` are the same two words the `any()`
entry scope uses, so one concept has one spelling in a declaration and in a
query. Neither `*` nor a selector can collide with a field name in a condition
path — there, the segment after a map is an entry key, and these would be that
key.

Three properties follow:

- A `*` may be followed by a repeated field **inside one entry**
  (`custom.*.value.labels.values`), which stores one entry per (key, element).
  That is not a cross product: the second fan lives inside one position of the
  first.
- Once the entry key is pinned by a predicate, the value **is** an ordering — a
  map holds at most one value per key, so the row occupies exactly one position.
  A list fan-out never gets this, which is why the two are distinguished.
- An ordering by a custom field that no predicate pins **declines to the scan**.
  Rows lacking that key sit outside every key range and have to sort last, and
  the executor has no second pass to append them.

A value component with **no key component beside it** is legal and useful — it is
what serves a cross-key `any()` — but a predicate naming one field then stays in
the residual: the range spans that value under every key, so the key constraint
has to be re-checked per row rather than dropped.

`indexed: true` on a map is refused: a field mark has no room to say which side,
which entry or which variant. The error points at the `*` form.

### What the admin UI covers

It renders and filters **scalar-valued** maps only. A message-valued map — the
shape user-defined fields take — is skipped in the row view, the edit form and
the filter bar. Everything above works the same either way; this is a gap in the
UI, not in the index or the query.

## Custom mergers for opaque fields

`WithMerger` on the server plus a client `MergerSpec` install a merge
function for one opaque field. It **must be idempotent, commutative and
associative**: it runs on server apply, on optimistic overlay merge, and again on
every rebase replay, so a non-idempotent merger (a concat, say) corrupts data.

For collaborative prose do not write one — use `text: true`, whose built-in
union merger satisfies the contract by construction.

## The option field numbers

The custom option field numbers (50100/50101) sit in the range reserved for use
within a single organization and are not registered. Two libraries claiming one
number is unrecoverable for an app depending on both — protobuf-go panics at
init with `extension number 50100 is already registered on message
google.protobuf.MessageOptions` — so claim numbers in protobuf's global
extension registry before publishing the protos. Moving them afterwards is a
breaking change for every adopter's generated code.
