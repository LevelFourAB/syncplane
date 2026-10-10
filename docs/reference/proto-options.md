# Proto options

Syncplane provides three custom Protobuf options in `syncplane/v1/options.proto` (published in the Buf module `buf.build/levelfourab/syncplane`) to declare how messages and fields participate in synchronization. For instructions on adding this file to code generation, see [Define entities in your own module](../how-to/define-entities.md).

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

| Field         | Description                                                                                                           |
| ------------- | --------------------------------------------------------------------------------------------------------------------- |
| `name`        | Logical entity name used on the wire and in storage (for example, `"todo"`).                                          |
| `key_fields`  | Field names, in order, whose values form the row key. Must be singular scalar fields.                                 |
| `index`       | Composite secondary indexes over this entity's fields. See [Secondary indexes](#secondary-indexes).                   |
| `server_only` | When `true`, rows never sync to clients. See [Server-only entities](#server-only-entities).                           |
| `history`     | Configures append-only field history tracking. See [Field history](#field-history).                                   |
| `access`      | The rules that decide who can read, create, update and delete rows of this entity. See [Access model](#access-model). |
| `grant`       | Declares the rows of this entity as grant rows. See [Access model](#access-model).                                    |
| `members`     | Declares the rows of this entity as teams, and names the entity whose rows make members. See [Teams](#teams).         |

The `name` field must be a valid identifier. Each entry in `key_fields` must reference a singular string, integer, boolean, or enum field. For complete syntax and constraints, see [Naming rules](naming.md).

A message configured with `(syncplane.v1.entity)` is an **entity**. Message-typed fields that reference this message type automatically become entity references without additional annotations. See [References](#references) for what a reference stores and how it resolves.

### Server-only entities

Setting `server_only: true` keeps an entity's rows on the server. The engine never bootstraps, pulls, or streams server-only rows to clients. Client subscriptions requesting a server-only entity are rejected with `invalid_argument`, and client CRUD operations targeting the entity are rejected. The TypeScript client reads this option from the schema and throws an error when constructing subscriptions to server-only entities.

```proto
message ProjectStats {
	option (syncplane.v1.entity) = {
		name: "project_stats"
		key_fields: [ "project_id" ]
		server_only: true
	};
	string project_id = 1;
	int64 open_issue_count = 2;
}
```

Server-only entities are queryable but not syncable. Server-side `ViewTx` and mutators can access the entity, and clients can query it using the `Query` RPC (`queryServer` in TypeScript). `server_only` controls replication, not access: the entity's read rules (see [Access model](#access-model)) and the application's [`ReadFilter`](../how-to/authentication-and-permissions.md#filtering-reads-and-writes-with-canmutate-and-readfilter) decide which rows a query returns. An entity outside the access model and without a `ReadFilter` returns its rows to every client with workspace access.

Client subscription filters cannot replace `server_only`. An empty subscription list requests a full sync, which syncs across browser tabs sharing local storage (such as IndexedDB). Only the `server_only` schema option prevents an entity from syncing to clients.

### Field history

Setting `history: {}` records an append-only log of field values before each editing session that modified them.

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

Field recording requires explicit annotation on each field. Registration fails if an entity enables `history: {}` without marking any fields.

Capture occurs on the server write path for all write sources, including client pushes, mutators, connectors, and trusted writes.

History records are stored in the `syncplane.v1.FieldHistory` entity (wire name `syncplane_field_history`), registered automatically when an entity enables history. This entity is `server_only` and does not sync to clients. Query it using `queryServer`. See [Restore a field to a previous value](../how-to/restore-a-field.md).

| `FieldHistory` field            | Description                                                                                                                                                                                                         |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `source_entity`, `source_key`   | The row that changed.                                                                                                                                                                                               |
| `seq`                           | The workspace sequence preceding the before-state (the sequence of the write that opened the record).                                                                                                               |
| `through_seq`, `writes`         | The last sequence folded into the record and the count of writes folded in (minimum 1). The record covers the span `seq`…`through_seq`. Spans for a single row can overlap across nodes.                            |
| `row_created`                   | Indicates that the row did not exist when the session began (`mask` and `prior` are empty). Read this field rather than checking for an empty `mask`, because history filters can also produce an empty mask.       |
| `mask`                          | Dotted Protobuf paths of the fields modified during the session.                                                                                                                                                    |
| `prior`                         | Serialized source entity containing key fields and the masked fields in their prior state. Text fields contain plain-string values rather than documents. Read using `plainTextOf` or `text.PlainText` for display. |
| `actor_subject`, `actor_system` | The actor who performed the write (see `storage.Actor`): caller subject, trusted write label, or both. Each session belongs to one actor. Unattributed writes leave both fields empty.                              |
| `recorded_at`                   | Server wall-clock timestamp when the record opened, used for display. Writes are ordered strictly by `seq`.                                                                                                         |

History recording rules:

- **Changes are recorded, not writes.** The server compares the existing row with the replacement row and records only modified fields. A write that makes no data changes produces no history record.
- **Sessions are recorded, not individual writes.** Consecutive writes to a row by the same actor fold into the initial session record: the record retains the before-state from the start of the session, expands its mask to include all touched fields, and increments `through_seq` and `writes`. The session window duration is configured by [`WithHistorySession`](server.md#field-history) (default: 5 minutes) from the time the record opened.
- A **`PATCH`** evaluates from its effective field mask, and a **`PUT`** evaluates from the complete capture set. Comparisons resolve at top-level field granularity. A oneof counts as a single field and is identified by its group name.
- **Key fields are never included in the mask.** A `PATCH` cannot modify key fields. The `prior` message retains key fields to identify the row.

For design trade-offs and performance implications, see [Limits and trade-offs](../explanation/limits-and-trade-offs.md#field-history-records-editing-sessions-not-keystrokes).

`DELETE` operations are not recorded in history and purge all existing history records for the row. A deleted row cannot be restored from field history. To support recoverable row deletion, model deletion as a status field. See [Recover a deleted row](../how-to/recover-a-deleted-row.md).

Removing `history: {}` stops capture for new writes but retains existing history records. The engine purges and expires those records according to retention settings as long as at least one entity in the schema enables history.

History records are read under the source row's permissions. See [Field history](server.md#field-history).

Retention policies and admission filters are configured with [`WithHistoryRetention` and `WithHistoryFilter`](server.md#field-history).

## `(syncplane.v1.field_behavior)` — on a field

```proto
bytes body   = 2 [(syncplane.v1.field_behavior) = { text: true }];
bytes blob   = 3 [(syncplane.v1.field_behavior) = { opaque: true }];
repeated Member members = 4 [(syncplane.v1.field_behavior) = { key_field: [ "uid" ] }];
repeated Item items = 5 [(syncplane.v1.field_behavior) = { key_field: [ "id" ] order_field: "pos" }];
```

| Field              | Type       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------ | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `opaque`           | `bool`     | Replaces the value as a whole rather than merging structurally. Used for CRDT blobs and atomic lists (order-sensitive, duplicate-bearing).                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `key_field`        | `repeated` | Identifies element key fields in a repeated message list (composite keys allowed). When empty on a registered entity element type, defaults to that entity's `key_fields`. Ignored for maps (keyed by map key) and scalar lists (keyed by value).                                                                                                                                                                                                                                                                                                                                   |
| `order_field`      | `string`   | Names the position field on the element that orders a keyed list (fractional index). Requires a keyed message list.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `text`             | `bool`     | Configures collaborative text with Peritext-style marks and anchors. Requires a singular `bytes` field; implies `opaque`. The engine registers the text merger automatically. Incompatible with `key_field` and `order_field`. May cross keyed message lists and message-valued maps (`notes.<key>.body`). Rejected inside a oneof or unkeyed repeated element. A write that moves a document's highest counter more than 2^32 above the stored document is rejected with `invalid_argument` (see [The counter ceiling](../explanation/collaborative-text.md#the-counter-ceiling)). |
| `indexed`          | `bool`     | Maintains a single-component secondary index on this field. On a repeated scalar field, produces one index entry per element. See [Secondary indexes](#secondary-indexes).                                                                                                                                                                                                                                                                                                                                                                                                          |
| `history`          | `bool`     | Records this field in entity history. Capture is explicit: only marked fields are recorded, and registration fails if an entity enables history without marking any fields. Marking any member of a oneof records the entire oneof group. Ignored if the entity does not enable `history`. See [Field history](#field-history).                                                                                                                                                                                                                                                     |
| `workspace`        | `bool`     | Inside a reference message, marks the string field that names the workspace the referenced row lives in. Empty means the holder's own workspace. Valid on one singular `string` field of a message carrying `(syncplane.v1.reference)`. See [References](#references).                                                                                                                                                                                                                                                                                                              |
| `reverse_relation` | `string`   | Names the reverse relation this reference adds to its target entity. Empty means `<plural(holder entity)>By<Field>`, such as `issuesByAssignee`. Valid on a reference field declared on an entity, including a member of a oneof. See [Reverse relations](#reverse-relations).                                                                                                                                                                                                                                                                                                      |
| `access_parent`    | `message`  | Places each row below the row this reference points at in the access model, or below each row a repeated reference points at. Set it as `access_parent: {}`. On a oneof, mark every member. See [Access model](#access-model).                                                                                                                                                                                                                                                                                                                                                      |

## References

A message-typed field whose (element) message is an entity is a **reference**. The stored row keeps only the target's key fields; the live target row resolves on read. A reference resolves inside the workspace of the row that holds it: a stub carries key fields alone, which identify a row within one partition only.

```proto
message Issue {
	option (syncplane.v1.entity) = { name: "issue" key_fields: [ "id" ] };
	string id = 1;
	Project project = 2; // stores project.id, resolves in the issue's workspace
	repeated Issue blocked_by = 3; // to-many; each element stores an id
}
```

Rules for every reference:

- The target must be a registered entity whose key fields sit outside any oneof.
- A query path may descend through a reference into its key fields only. A comparison on the reference itself compares its key (single-key targets only); `matches()` crosses to the live target row.
- The engine maintains one secondary index per reference field, named `__ref_<field>`, over the stored target key.
- A reference may be a member of a oneof; the oneof is then a [polymorphic reference](#polymorphic-references).
- The reference adds a [reverse relation](#reverse-relations) to its target entity.

### Reverse relations

A **reverse relation** lists the rows of the holder entity that point at one target row. It lives on the target entity, and a `follow` segment, an `include` key and the client's `refs` accessor all use its name.

The name comes from the reference field alone:

- The field's `reverse_relation`, when set.
- Otherwise, `<plural(holder entity)>By<Field>` in camelCase. `Field` is the name of the entity's own field that holds the reference: the member for a member of a oneof. For the plural, a trailing `y` after a character other than `a`, `e`, `i`, `o` or `u` becomes `ies`, a name that ends in `s`, `x`, `z`, `ch` or `sh` takes `es`, and any other name takes `s`. The entity `task_list` with the field `created_by` gives `taskListsByCreatedBy`.

```proto
message Issue {
	option (syncplane.v1.entity) = { name: "issue" key_fields: [ "id" ] };
	string id = 1;
	Project project = 2; // Project gets `issuesByProject`
	User assignee = 3; // User gets `issuesByAssignee`
	User reporter = 4 [(syncplane.v1.field_behavior) = { reverse_relation: "reportedIssues" }];
}
```

| Rule        | Detail                                                                                                                                                                                                                                                                                                        |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stability   | A name never depends on other fields. Adding a reference does not rename an existing reverse relation.                                                                                                                                                                                                        |
| Uniqueness  | On one target entity, two reverse relations cannot share a name, and a reverse relation cannot share the name of a reference field of that entity. Field names are unique, so a default name clashes only when two cases of one [polymorphic reference message](#polymorphic-references) point at one entity. |
| Name syntax | The [entity-name rule](naming.md#reverse-relation-names): an ASCII letter or `_`, followed by ASCII letters, digits or `_`, at most 64 characters.                                                                                                                                                            |
| Placement   | Valid only on a reference field declared on an entity, including a member of a oneof. Registration refuses it on any other field, including the fields inside a reference message.                                                                                                                            |
| Clash       | The TypeScript client throws when it is constructed. The server refuses each request that follows a relation with `failed_precondition`. Both errors name the fields that claim the name.                                                                                                                     |

### `(syncplane.v1.reference)` — workspace-qualified references

A **reference message** is the stored shape of a reference that may point into another workspace. It declares `(syncplane.v1.reference)` and wraps an entity-typed field together with an optional workspace field:

```proto
message PersonRef {
	option (syncplane.v1.reference) = {};
	string workspace_id = 1 [(syncplane.v1.field_behavior) = { workspace: true }];
	Person person = 2;
}

message Task {
	option (syncplane.v1.entity) = { name: "task" key_fields: [ "id" ] };
	string id = 1;
	PersonRef assignee = 2;
	repeated PersonRef watchers = 3;
}
```

A field of a reference message type is a reference like any other: it gets the forward accessor, the reverse relation on the target, the follow segment, and the derived index. Its target is the entity the embedded field names.

| Rule            | Detail                                                                                                                                                                                                                                                                                                                                                                        |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Shape           | Exactly one singular entity-typed field — or a oneof whose members are all singular entity-typed fields, see [polymorphic references](#polymorphic-references) — at most one singular `string` field marked `workspace` outside any oneof, and no other fields. The message is not itself an entity. Registration rejects anything else.                                      |
| Cardinality     | The holder field decides: `PersonRef assignee` is to-one, `repeated PersonRef watchers` is to-many. Each element names its own workspace.                                                                                                                                                                                                                                     |
| Workspace value | An empty or absent workspace means the holder's own workspace, so a reference message without the workspace field is a same-workspace reference with an explicit stored shape. Existing schemas keep their meaning. A value that fails the [workspace-id rule](naming.md#workspace-ids) is refused as `invalid_argument`.                                                     |
| Stored value    | The workspace field as written, and the embedded stub projected down to the target's key fields.                                                                                                                                                                                                                                                                              |
| Query paths     | `assignee.person.id` compares the key, `assignee.workspace_id` the workspace; `assignee` as a comparison operand extends to `assignee.person.id`. Any other field below `person` is rejected. `matches()` names `assignee`, never `assignee.person`, which would drop the workspace test.                                                                                     |
| Merge           | Replaced whole on every write. A `PATCH` mask that names one, or a sub-path into one, replaces the whole message, so the workspace and the key never split. A repeated reference message merges as a set: a mask path `watchers.<workspace>/<key>` adds or removes that one element, and an element with an empty workspace and one naming the holder's own are two elements. |
| Client reads    | Forward accessors, `include`, reverse relations, and `matches()` resolve each stub in the workspace it names, from the workspaces the client holds. A target the client does not hold reads as absent.                                                                                                                                                                        |
| Server reads    | `matches()`, `include`, and `follow` resolve inside the request's workspace only. A stub that names another workspace resolves to nothing there, and never to a same-key row in the request's workspace.                                                                                                                                                                      |

A list whose elements carry more than the reference wraps each one in a keyed list element of its own:

```proto
message Watcher {
	string id = 1;
	PersonRef person = 2;
}
message Task {
	// …
	repeated Watcher watchers = 3 [(syncplane.v1.field_behavior) = { key_field: [ "id" ] }];
}
```

The TypeScript schema builder declares the same shape with `schema.workspaceRef(() => person)` and `schema.workspaceRefList(() => person)`; see the [TypeScript client reference](typescript-client.md#workspace-qualified-references).

### Polymorphic references

A **polymorphic reference** points to one of several target entities. Declare a polymorphic reference as a oneof of references in one of two locations:

```proto
// On the holder: each member is an entity-typed field or a reference message.
message Task {
	option (syncplane.v1.entity) = { name: "task" key_fields: [ "id" ] };
	string id = 1;
	oneof parent {
		Client client = 2;
		AreaRef area = 3; // a reference message, so this case may name another workspace
		string label = 4; // a plain member, not a reference
	}
}

// Inside a reference message: the target is a oneof beside the workspace.
message OwnerRef {
	option (syncplane.v1.reference) = {};
	string workspace_id = 1 [(syncplane.v1.field_behavior) = { workspace: true }];
	oneof owner {
		Client client = 2;
		Area area = 3;
	}
}
message Memo {
	option (syncplane.v1.entity) = { name: "memo" key_fields: [ "id" ] };
	string id = 1;
	OwnerRef owner = 2;
	repeated OwnerRef mentions = 3;
}
```

Each reference member is a **case** of the relation. A row holds at most one case. Inactive cases evaluate as unset references.

| Rule              | Detail                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Relation name     | The name of the oneof on the holder (`parent`), or the field name of a reference message (`owner`). This name is the forward accessor and the segment used by `follow` and `include`. A case has no relation name of its own.                                                                                                                                                                                                         |
| Cases             | A oneof on the holder can mix entity-typed fields, reference messages, and plain fields. Only the references are cases. A oneof inside a reference message must contain singular entity-typed fields only. A polymorphic reference message cannot be a member of a oneof on the holder.                                                                                                                                               |
| Reverse relations | One per case, on that case's target entity. A member of the holder's oneof is its own field and takes its own name (`tasksByClient` on `Client`, `tasksByArea` on `Area`). A reference message field gives its name to every case (`memosByOwner` on `Client` and on `Area`). Two cases of one reference message that point at one entity therefore clash; hold them in separate fields. See [Reverse relations](#reverse-relations). |
| Indexes           | One derived index per case: `__ref_<field>` for a member of the holder's oneof, and `__ref_<field>.<case>` for a case inside a reference message.                                                                                                                                                                                                                                                                                     |
| Query paths       | A comparison names the case: `client.id` on the holder, or `owner.client.id` inside a reference message. `matches()` also names the case (`client` or `owner.client`), never the oneof or reference message. Query a case of a repeated reference message through the list: `mentions.client`.                                                                                                                                        |
| Merge             | The holder's oneof replaces as a whole when a `PATCH` mask names the group. A polymorphic reference message also replaces as a whole. A repeated reference message merges as a set with element identity `<workspace>/<case>/<key>`. Two elements with the same key under different cases remain distinct.                                                                                                                            |
| Workspaces        | In a reference message, `workspace_id` sets the workspace for all cases. In a holder oneof, each reference message case specifies its own workspace. All [workspace-qualified reference rules](#syncplanev1reference--workspace-qualified-references) apply per case.                                                                                                                                                                 |

## Access model

Four options declare who can read and write rows: `access`, `grant` and `members` on an entity, and `access_parent` on a reference field.

```proto
// A role on the whole workspace.
message OrgMember {
	option (syncplane.v1.entity) = {
		name: "org_member"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [
				{ name: "org_member" grants: [ "view_projects", "view_tasks" ] },
				{ name: "org_admin" includes: [ "org_member" ] grants: [ "view_members", "manage_members", "edit_projects", "edit_tasks" ] }
			]
		}
		access: {
			allow: [
				{ read: true requires: "view_members" },
				{ write: true requires: "manage_members" }
			]
		}
	};
	string id = 1;
	string user_id = 2;
	string role = 3;
}

message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_projects" },
				{ write: true requires: "edit_projects" }
			]
		}
	};
	string id = 1;
}

// A role on one project and the rows below it.
message ProjectMember {
	option (syncplane.v1.entity) = {
		name: "project_member"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [
				{ name: "project_reader" grants: [ "view_projects", "view_tasks" ] },
				{ name: "project_writer" grants: [ "view_projects", "view_tasks", "edit_tasks" ] }
			]
		}
		access: {
			allow: [
				{ read: true requires: "view_members" },
				{ write: true requires: "manage_members" }
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string user_id = 3;
	string role = 4;
}

message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_tasks" },
				{ write: true requires: "edit_tasks" }
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
}

// Every caller admitted to the workspace reads labels. Org admins write them.
message Label {
	option (syncplane.v1.entity) = {
		name: "label"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true admitted: {} },
				{ write: true requires: "edit_projects" }
			]
		}
	};
	string id = 1;
}
```

A caller holds a permission at an **anchor**: the workspace root, or one row that a grant row's `access_parent` field points at. A permission held at an anchor applies to the anchor row and to every row below it: the rows whose `access_parent` fields point at it, the rows whose `access_parent` fields point at those, and so on. A comment whose parent is a task, whose parent is a project, is below the project. An entity can be its own access parent, such as a folder inside a folder.

| Option                         | Field       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------ | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `access`                       | `allow`     | The rules (`syncplane.v1.AccessRule`). At least one. Two rules with the same operation flags, source and checks are refused.                                                                                                                                                                                                                                                                                                                   |
| `access.allow[]`               | `read`      | Admits reading rows.                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `access.allow[]`               | `create`    | Admits creating rows.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `access.allow[]`               | `update`    | Admits changing rows that exist, which includes moving a row to another access parent.                                                                                                                                                                                                                                                                                                                                                         |
| `access.allow[]`               | `delete`    | Admits deleting rows.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `access.allow[]`               | `write`     | Admits `create`, `update` and `delete`, and each write operation that a later version of `options.proto` adds.                                                                                                                                                                                                                                                                                                                                 |
| `access.allow[]`               | `requires`  | The source: a caller that holds every permission in this list at the workspace root, at the row, or at a row above the row through its access parents. Each permission can be held at a different one of these places. No permission twice. The text format writes one permission without brackets: `requires: "view_tasks"`. Set exactly one of `requires` and `admitted`. See [Several permissions together](#several-permissions-together). |
| `access.allow[]`               | `admitted`  | The source: every caller admitted to the workspace. Set it as `admitted: {}`. A caller is admitted when `CanAccessWorkspace` lets it in and, in a schema that declares a role, it holds at least one permission in the workspace.                                                                                                                                                                                                              |
| `access.allow[]`               | `caller_is` | A check: the named top-level, singular `string` field holds one of the caller's subjects. An empty value never matches.                                                                                                                                                                                                                                                                                                                        |
| `access.allow[]`               | `fields`    | A check: an update changes no top-level field or oneof outside this list. Only on a rule whose one operation is `update`. Cannot name a key field. A `PUT` keeps the stored value of each field the caller's rules do not list; see [Field guards](#field-guards).                                                                                                                                                                             |
| `access.allow[]`               | `when`      | A check: the row matches this `syncplane.v1.Condition`, with paths from the row. A `rel` or `match` condition is refused. See [Conditions on the row](#conditions-on-the-row).                                                                                                                                                                                                                                                                 |
| `access.allow[]`               | `within`    | Checks: each entry (`syncplane.v1.AccessWithin`) holds. See [Checks on the rows above](#checks-on-the-rows-above).                                                                                                                                                                                                                                                                                                                             |
| `access.allow[].within[]`      | `entity`    | The entity of the row the row must sit within, by name: an entity whose rows the rule's rows can sit below, or the rule's own entity when its rows sit below rows of their own.                                                                                                                                                                                                                                                                |
| `access.allow[].within[]`      | `caller_is` | A check: the named top-level, singular `string` field of that row holds one of the caller's subjects. An empty value never matches.                                                                                                                                                                                                                                                                                                            |
| `access.allow[].within[]`      | `when`      | A check: that row matches this `syncplane.v1.Condition`, with paths from that row. A `rel` or `match` condition is refused.                                                                                                                                                                                                                                                                                                                    |
| `access`                       | `guard`     | The field guards (`syncplane.v1.FieldGuard`). Each restricts who reads and who writes a set of fields. See [Field guards](#field-guards).                                                                                                                                                                                                                                                                                                      |
| `access.guard[]`               | `fields`    | The top-level fields and oneofs the guard restricts. At least one. Cannot name a key field, an access parent or a `creator` field, or a field another guard names.                                                                                                                                                                                                                                                                             |
| `access.guard[]`               | `allow`     | The guard's rules, with the fields of `access.allow[]` except `delete` and `fields`. Without a rule that names `read`, the guard hides its fields from every client. Can be empty.                                                                                                                                                                                                                                                             |
| `grant`                        | `subject`   | The name of the field or oneof that holds the subject the row grants to. A field is top-level and singular: a `string` field names one caller, and a reference to an entity with `members` names a team. See [Teams](#teams).                                                                                                                                                                                                                  |
| `grant`                        | `role`      | The name of the field that holds the role the row grants: a top-level, singular `string` or enum field outside any oneof. An enum value names the role with the same name as the value.                                                                                                                                                                                                                                                        |
| `grant`                        | `roles`     | The roles rows can grant. Each role has a `name`, the permissions it `grants`, and the names of the roles of the same `grant` it `includes`. A role grants every permission of each role it includes, through any number of steps.                                                                                                                                                                                                             |
| `members`                      | `entity`    | The name of the grant entity whose rows make the members of each row of this entity. See [Teams](#teams).                                                                                                                                                                                                                                                                                                                                      |
| `field_behavior.access_parent` | (none)      | Marks a reference that places each row below another row, which can have an access parent of its own. See [Several access parents](#several-access-parents). A grant row anchors at the row it points at. A grant entity without the field anchors at the workspace root.                                                                                                                                                                      |

An entity whose rules use `admitted` is in the model like every other entity: it can have an access parent, be the access parent of other entities, and be a grant entity.

A grant row grants nothing when its subject is empty or names a team that does not exist, its role is not declared in `roles`, or its `access_parent` field is unset or names a row that does not exist. Every write path refuses to store a grant row with no subject, an undeclared role, or no `access_parent` on an entity that has one.

A **subject** is one identity of a caller, such as a user id or a verified email address. The server maps each caller to its subjects with `WithCallerAccess` (see [Access model](server.md#access-model)). Three options name a field that holds a subject:

| Option                              | Reads a field of                        | Meaning                                                |
| ----------------------------------- | --------------------------------------- | ------------------------------------------------------ |
| `grant.subject`                     | The grant row                           | Who receives the row's role: one subject, or a team.   |
| `access.allow[].caller_is`          | The row that the rule decides           | A check: the field holds one of the caller's subjects. |
| `access.allow[].within[].caller_is` | The row of `entity` at or above the row | The same check, on that row.                           |

### How rules admit an operation

An operation on a row is allowed when at least one rule names the operation, its source holds for the caller, and each check it sets holds on the row. The order of the rules has no effect, and no rule removes access that another rule gives. A `requires` source holds at a row when the caller holds each permission in the list at the workspace root, at the row, or at a row above the row through its access parents. An `admitted` source holds at every row.

| Operation | The source and checks of one rule that names the operation must hold at                                                               |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Read      | The row.                                                                                                                              |
| Create    | The row as it will be stored.                                                                                                         |
| Delete    | The stored row.                                                                                                                       |
| Update    | The stored row and the row as it will be stored. One rule must hold at both. Two rules that each hold at one of them do not admit it. |

A write that moves a row to another access parent also needs, where the stored row sits, the right to read every entity whose rows can sit below it. A delete of a row that a live row names as its access parent, or that a live grant row names as its subject, is refused. A write that can change what a grant row grants needs every permission of the role at the row's anchor, before and after the write, so a caller grants and revokes only what it holds itself. See [Access model](server.md#access-model) for the full write rules and for mutators.

### Several permissions together

Rules join with OR: an operation is allowed when any rule admits it. The permissions in one rule's `requires` join with AND: the caller must hold each of them. Each permission holds at the workspace root, at the row, or at a row up the row's chain, and each can hold at a different one of these.

```protobuf
message Task {
  option (syncplane.v1.entity) = {
    name: "task" key_fields: ["id"]
    access: { allow: [
      { read: true requires: "view_tasks" },
      { update: true requires: ["edit_tasks", "use_billing"] }
    ] }
  };
  string id = 1;
  Project project = 2 [(syncplane.v1.field_behavior).access_parent = {}];
}
```

A caller with `edit_tasks` at the task's project and `use_billing` at the workspace root updates the task. A caller with only one of the two does not. A move needs every permission of one rule at the old place and at the new one. To admit a caller that holds either of two permissions, declare two rules. A rule that must hold `A` and one of `B` and `C` is the two rules `["A", "B"]` and `["A", "C"]`.

The order of `requires` has no effect: two rules that require the same permissions in another order are the same rule.

A read rule that requires several permissions narrows bootstrap, resync and `Query` to the rows below the anchors of each permission that the caller does not hold at the workspace root. A cursor records the anchors of each of them, so a grant or a revoke of any one resyncs the rows it shows or hides.

### Conditions on the row

A `when` check admits only rows that match a condition. The condition is a `syncplane.v1.Condition`, the predicate language of queries and subscriptions, and reads only the row: a `rel` condition, which reads another row, and a `match` condition are refused at registration. A permission held at the workspace root does not skip the check.

```protobuf
message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_tasks" },
				{
					update: true
					requires: "edit_tasks"
					when: {
						cmp: {
							path: [ "archived" ]
							op: CMP_OP_EQ
							value: { bool_value: false }
						}
					}
				},
				{ update: true requires: "archive_tasks" }
			]
		}
	};
	string id = 1;
	bool archived = 2;
}
```

A holder of `edit_tasks` edits a task while it is not archived. The update rule must admit the stored row and the row as it will be stored, so the rule also refuses the write that archives a task and the write that restores one. A holder of `archive_tasks` does both.

| Operation | Effect of `when`                                                                                                                                                    |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Read      | A row that a write makes fail the condition leaves every client that the rule alone let read it, as a delete. A row that a write makes match reaches those clients. |
| Create    | The row as it will be stored must match.                                                                                                                            |
| Delete    | The stored row must match.                                                                                                                                          |
| Update    | The stored row and the row as it will be stored must match.                                                                                                         |

A read rule with `when` does not narrow the rows that bootstrap, resync and `Query` read: they read the rows below the rule's anchors, or every row of the entity when the caller holds every permission of the rule at the workspace root, and test each one.

### Checks on the rows above

A `within` entry admits only rows that sit within a row that passes a check: the row itself, or a row above it on any path of access parents, is a row of the named entity, and that row passes every check the entry sets. A row with no such row at or above it fails the entry. The entries of a rule join with AND, and with the rule's other checks. A permission held at the workspace root does not skip the check.

```protobuf
message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_tasks" },
				{
					read: true
					requires: "view_open_tasks"
					within: {
						entity: "project"
						when: {
							cmp: {
								path: [ "private" ]
								op: CMP_OP_EQ
								value: { bool_value: false }
							}
						}
					}
				},
				{
					write: true
					requires: "edit_tasks"
					within: {
						entity: "project"
						when: {
							cmp: {
								path: [ "archived" ]
								op: CMP_OP_EQ
								value: { bool_value: false }
							}
						}
					}
				}
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
}

message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_tasks" },
				{
					read: true
					write: true
					admitted: {}
					within: { entity: "project" caller_is: "owner_id" }
				}
			]
		}
	};
	string id = 1;
	Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
}
```

A role at the workspace root that grants `view_open_tasks` reads the tasks of every project that is not private. A role at a project that grants `view_tasks` reads its tasks whether it is private or not. No caller writes a task of an archived project. The caller that a project names in its `owner_id` field reads and writes the comments of its tasks.

| Operation | Effect of `within`                                                                                                                                                                                |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Read      | A write to a row above that makes the row fail every entry that let a client read it removes the row from that client, as a delete. A write that makes it pass sends the row.                     |
| Create    | The row as it will be stored must sit within a row that passes.                                                                                                                                   |
| Delete    | The stored row must sit within a row that passes.                                                                                                                                                 |
| Update    | The stored row and the row as it will be stored must sit within a row that passes. A move out of a row that fails, or into one, is refused.                                                       |
| Above     | A write that changes whether a row passes an entry needs, where the row sits, a read rule of each entity whose rows can sit below it, as a move does. See [Access model](server.md#access-model). |

The rows above a row are a graph when a row has several parents. One row that passes is enough, so a parent only adds. In an entity whose rows sit below rows of their own, such as a folder tree, a row that fails does not hide the rows below it from a caller when a row above it passes: a private inner folder below a folder that is not private does not hide its documents. Give the inner row grants of its own instead.

A read rule with a `within` entry narrows bootstrap, resync and `Query` to the rows below the rule's anchors. When the caller holds every permission of the rule at the workspace root, or the rule uses `admitted`, an entry with `caller_is` narrows to the rows within the rows whose field names the caller, when that field has an index: mark it `indexed`. Otherwise the read tests every row of the entity.

### Field guards

The rules in `allow` decide whole rows. A **field guard** takes a set of fields away from every rule, and gives them back to the callers its own rules admit, for reading and for writing each. A write that sets a guarded field needs a rule in `allow` that admits the write, and a rule of the guard that admits the field operation: `create` for the value a create stores, `update` for a change to a row that exists. `write` admits both. A field operation that no rule of the guard names is open only to trusted writes.

```proto
message Expense {
	option (syncplane.v1.entity) = {
		name: "expense"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_tasks" },
				{ write: true requires: "edit_tasks" }
			]
			guard: [
				// Only server code sets the total.
				{ fields: [ "total" ] allow: [ { read: true admitted: {} } ] },
				// Only a caller with edit_projects sets the status.
				{
					fields: [ "status" ]
					allow: [
						{ read: true admitted: {} },
						{ write: true requires: "edit_projects" }
					]
				},
				// Only the reporter writes the notes.
				{
					fields: [ "notes" ]
					allow: [
						{ read: true admitted: {} },
						{ write: true requires: "edit_tasks" caller_is: "reporter_id" }
					]
				}
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string title = 3;
	string status = 4;
	int64 total = 5;
	string reporter_id = 6 [(syncplane.v1.field_behavior) = { creator: {} }];
	string notes = 7;
}
```

A guard rule admits a field operation as a rule in `allow` admits a row operation: its source holds for the caller where the row sits, and each check it sets holds on the row as it will be stored and, for an update, on the stored row. A `caller_is` or `when` check of a guard rule reads only fields that no guard names.

| Write                                    | Outcome when no guard rule admits the caller                      |
| ---------------------------------------- | ----------------------------------------------------------------- |
| `PATCH` whose mask writes into the field | Refused with `access_denied`. The rest of the mask does not land. |
| `PUT` that updates a row                 | Applied. The field keeps its stored value.                        |
| `PUT` that creates a row                 | Applied. The field gets its zero value.                           |
| `DELETE`                                 | Not affected. A guard restricts a value, not the row.             |

A mask path matches a guard by its first segment: a guard on `audit` covers `audit.size`. A guard on a oneof covers each of its members. A `PATCH` that names a field whose guard has no rule for `update` is refused before the row is read, and before `Permissions.CanMutate` runs.

Clients edit a guarded collaborative-text field only where a guard rule admits an update: a `PUT` keeps the stored document.

#### Who reads a guarded field

A caller reads a guarded field of a row when a rule in `allow` admits reading the row, and a rule of the guard that names `read` admits reading the row too. A guard rule admits a read as a rule in `allow` does: its source holds where the row sits, and each check holds on the row. The rule `{ read: true admitted: {} }` shows the field to every caller that reads the row. A guard without a read rule hides its fields from every client.

```proto
message Staff {
	option (syncplane.v1.entity) = {
		name: "staff"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_staff" },
				{ write: true requires: "edit_staff" }
			]
			guard: [
				// Only HR reads and writes the salary.
				{ fields: [ "salary" ] allow: [ { read: true write: true requires: "hr" } ] },
				// Only the person a record is about reads the review.
				{
					fields: [ "review" ]
					allow: [
						{ read: true admitted: {} caller_is: "person_id" },
						{ write: true requires: "hr" }
					]
				},
				// No client reads or writes the bonus.
				{ fields: [ "bonus" ] }
			]
		}
	};
	string id = 1;
	string name = 2;
	int64 salary = 3;
	string review = 4;
	string person_id = 5;
	int64 bonus = 6;
}
```

A guard decides per row and per caller. The server clears each field the caller may not read before the row leaves it, and names the field in `RowChange.hidden`. A cleared field reads as its zero value; the list tells it apart from a stored zero value. This applies to every path that sends a row: pull, bootstrap, `Subscribe`, `Query`, field history and the in-process `View` and `Apply` reads. In a field history record, a hidden field leaves the mask and the prior value, and a record of hidden fields alone is not shown.

| Change                                                                         | What the client receives                                                  |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| A grant, a role or a move above the row shows or hides a field                 | The row again, with its new `hidden` list. A text field arrives whole.    |
| A write changes a field that a guard read rule checks, such as its `caller_is` | The row, as for any write.                                                |
| A write changes a hidden field                                                 | The row, with the field still cleared. No text delta state for the field. |

A request may not name a field that the caller does not read on every row it reads: a read rule of the guard that needs no permission and sets no check, or whose permissions the caller holds at the workspace root. Otherwise the server refuses it with `PermissionDenied`:

- a `Query` predicate, `order_by`, `group_by` or facet, including the inner condition of a `rel` on the target entity;
- a subscription predicate, a window's `order_by`, or a reference that a `follow` path walks;
- a `match` condition or a relevance order, when the guard restricts a search field.

A subscription that a grant change makes refused fails on the next pull.

Reading and writing are separate, so a caller can write a field it may not read. A `PATCH` whose mask names the field sets it. A `PUT` that updates a row keeps the stored value of each such field that it leaves unset, so a client that writes back a row it received does not clear what it never saw. A `PUT` that sets such a field writes it, when a guard rule admits the caller. To clear a field the caller may not read, use a `PATCH`.

Registration refuses a guard that can hide a field a client needs to decide access: a field that a rule in `allow` checks with `caller_is` or `when`, a field that a `within` entry of any rule reads, and the subject and role fields of a grant entity. A guard can hide a field only when some read rule of the guard does not admit every reader with no check.

A rule's `fields` list and a guard work in opposite directions:

| You want                                                                                   | Declare                                  |
| ------------------------------------------------------------------------------------------ | ---------------------------------------- |
| A caller who can change only some fields of a row, such as an assignee who sets the status | `fields` on an `update` rule in `allow`  |
| A field that only some callers can change, whatever rule admits the update                 | A guard on the field                     |
| A field that only server code changes                                                      | A guard whose only rule is the read rule |
| A field that only some callers read                                                        | A guard with a narrower read rule        |
| A field that no client reads                                                               | A guard without a read rule              |

A `PUT` that updates a row also keeps the stored value of each field outside the `fields` lists of the update rules that admit the caller at the stored row, when each of those rules has a list. A `PATCH` that names such a field is refused, as before.

The TypeScript client refuses at the call site a `patch()` that names a field whose guard has no rule for `update`, and keeps the local value of such a field on `put()`. Whether a guard rule admits the caller is decided on the server; `can()` predicts it, for an update and for a read that names `fields`. A row the client holds lists its hidden fields in `Row.hidden`; see the [TypeScript client reference](typescript-client.md). For a field that depends on facts outside the workspace's rows, use the server's `FieldPermissions` hook; see [Authentication and permissions](../how-to/authentication-and-permissions.md#protecting-fields-per-caller).

### Operations no rule admits

An operation that no rule of an entity names is open only to trusted writes: in-process writes under `server.Trusted`, connectors, effects, and the bodies of custom mutators. Before the write transaction opens, and before `Permissions.CanMutate` runs, the server refuses with `access_denied` a write that can only be such an operation. This check reads no row. It classifies the write from its operation and its `expected_version`, where version 0 means that the row does not exist:

| Write                                  | The write is                                                        | Refused when no rule names  |
| -------------------------------------- | ------------------------------------------------------------------- | --------------------------- |
| `PATCH`                                | An update. A `PATCH` never creates a row.                           | `update`                    |
| `DELETE`                               | A delete.                                                           | `delete`                    |
| `PUT` with `expected_version: 0`       | A create.                                                           | `create`                    |
| `PUT` with non-zero `expected_version` | An update.                                                          | `update`                    |
| `PUT` with no `expected_version`       | A create when the row does not exist, and an update when it exists. | either `create` or `update` |

The write transaction then checks a `PUT` with no `expected_version` as the operation it is on the stored row. The TypeScript client makes the same classification and rejects the write at the call site, so the write does not enter the pending queue.

In a schema that does not use the access model, the server refuses no write for its operation. The application's `CanMutate` hook sees the entity and the operation of each write and can refuse it. See [Reserve an operation for trusted writes](../how-to/authentication-and-permissions.md#reserve-an-operation-for-trusted-writes).

Additional constraints:

- A rule reads the permissions the caller holds where the row sits, the fields of the row itself, and, through `within`, the fields of the rows at or above it. It never reads a field of a row below the row or beside it.
- Rules in `allow` apply to whole rows. To restrict who writes one field of a writable row, use a [field guard](#field-guards).
- The rows that a custom mutator body writes are not checked against the rules. The invocation is checked by its `MutatorAccess`; see [Access model](server.md#access-model). A mutator can therefore make a change that no rule admits to the caller:

  ```go
  server.WithMutator("archiveProject", &appv1.ArchiveArgs{},
  	func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
  		// … whatever must happen first …
  		return tx.Delete(&appv1.Project{}, args.(*appv1.ArchiveArgs).GetId())
  	},
  	server.MutatorAccess{Permission: "edit_projects", Entity: "project", KeyField: "id"})
  ```
- A batch applies its writes in sequence. A `DELETE(k)` followed by `PUT(k, expected_version: 0)` is a delete and a create, and replaces the row. A caller that a delete rule and a create rule admit can therefore replace a row without an update rule.
- Clients edit collaborative-text fields with `PATCH`, so a client edits them only where an update rule admits it.

### Several access parents

A row can sit below several rows. Mark several reference fields, a repeated reference, or both. A permission held at any row a marked field names, or at a row above it, reaches the row.

```proto
// A task in a project and in a milestone, with labels that each share it.
message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_tasks" },
				{ write: true requires: "edit_tasks" },
				{
					update: true
					requires: "plan_tasks"
					fields: [ "milestone" ]
				}
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	Milestone milestone = 3
		[(syncplane.v1.field_behavior) = { access_parent: {} }];
	repeated Label labels = 4
		[(syncplane.v1.field_behavior) = { access_parent: {} }];
}
```

| Rule           | Description                                                                                                                                                                                                                                                                                     |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Parents        | The rows every marked field names. A row named twice is one parent. A row whose marked fields name no row has no parent.                                                                                                                                                                        |
| Read           | The source of a read rule holds through any parent.                                                                                                                                                                                                                                             |
| Create         | The source of the create rule must hold through each parent the row names: each permission at the workspace root, at the row, or at that parent or a row above it.                                                                                                                              |
| Add, remove    | An update that adds or removes a parent needs the source of its rule through each parent it adds and each parent it removes, in addition to the update rules above. A move from one parent to another is one removed and one added. A rule with `fields` admits it only when it lists the unit. |
| Other writes   | An update that leaves the parents as they are, and a delete, need the source through any parent.                                                                                                                                                                                                |
| Lists          | A `PATCH` adds or removes one element with the mask path `labels.<key>`, and two concurrent additions both stay. A `PUT` replaces the list.                                                                                                                                                     |
| Delete         | A row cannot be deleted while a live row names it as a parent, also when that row has other parents.                                                                                                                                                                                            |
| Limits         | A write fails with `invalid_argument` when the row would name more rows than `Limits.MaxAccessParents`. `Limits.MaxAccessDepth` applies to the longest path above a row. See [Limits](server.md#limits).                                                                                        |
| Grant entities | A grant entity has one access parent: one singular field, or the members of one oneof.                                                                                                                                                                                                          |

### Polymorphic access parents

An access parent can point at one of several entities. Declare it in either shape of a [polymorphic reference](#polymorphic-references): mark every member of a oneof on the entity, or mark a field whose reference message holds a oneof of entities.

```proto
// A comment on a task, or on a doc.
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true requires: "view_comments" },
				{ write: true requires: "edit_comments" }
			]
		}
	};
	string id = 1;
	oneof subject {
		Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
		Doc doc = 3 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	}
}
```

| Rule        | Description                                                                                                                                                                                                                    |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Parent row  | The case a row holds names the row it sits below. A row that holds no case, or holds a case that names no key, has no parent.                                                                                                  |
| Permissions | The entity's rules apply in every case. A role reaches the rows below a target only when it grants the permissions of the rules, so grant `view_comments` both in the roles that reach tasks and in the roles that reach docs. |
| Cases       | Every member of the oneof is marked, and every member is a reference. A reference message with a `workspace` field is refused.                                                                                                 |
| Moves       | A write that changes the case or the key is a move, with the update rules above. A task and a doc with the same key are two rows.                                                                                              |
| Masks       | A `PATCH` reaches the access parent when its mask names the oneof group (`subject`), or the field that holds the reference message.                                                                                            |
| Depth       | `Limits.MaxAccessDepth` applies to the deepest chain of any case.                                                                                                                                                              |

### Teams

A grant row can name a team as its subject. The row then grants its role to every member of the team. Declare the team entity with `members`, which names the grant entity whose rows make members:

```proto
message Team {
	option (syncplane.v1.entity) = {
		name: "team"
		key_fields: [ "id" ]
		members: { entity: "team_member" }
		access: {
			allow: [
				{ read: true requires: "view_teams" },
				{ write: true requires: "manage_teams" }
			]
		}
	};
	string id = 1;
}

message TeamMember {
	option (syncplane.v1.entity) = {
		name: "team_member"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [
				{ name: "team_member" grants: [ "view_teams" ] },
				{ name: "team_lead" grants: [ "view_teams", "manage_teams" ] }
			]
		}
		access: {
			allow: [
				{ read: true requires: "view_teams" },
				{ write: true requires: "manage_teams" }
			]
		}
	};
	string id = 1;
	Team team = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string user_id = 3;
	string role = 4;
}

// A role on one project for one user, or for every member of one team.
message ProjectMember {
	option (syncplane.v1.entity) = {
		name: "project_member"
		key_fields: [ "id" ]
		grant: {
			subject: "member"
			role: "role"
			roles: [
				{ name: "project_reader" grants: [ "view_projects", "view_tasks" ] },
				{ name: "project_writer" grants: [ "view_projects", "view_tasks", "edit_tasks" ] }
			]
		}
		access: {
			allow: [
				{ read: true requires: "view_projects" },
				{ write: true requires: "edit_projects" },
				{ delete: true requires: "view_projects" caller_is: "user_id" }
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	oneof member {
		string user_id = 3;
		Team team = 4;
	}
	string role = 5;
}
```

| Rule          | Description                                                                                                                                                                                                                 |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Membership    | A row of the member entity that grants its role at a team row also makes its subject a member of that team, whatever the role. A permission held at the workspace root, or at a row above the team, does not make a member. |
| Subject       | `grant.subject` names a `string` field, a reference to an entity with `members`, or a oneof whose members are each one of these. A row that holds no case grants nothing.                                                   |
| Member entity | A grant entity whose `access_parent` points at the team entity alone, and whose subject is one `string` field. A team therefore cannot be a member of a team.                                                               |
| Subset rule   | Unchanged. A write to a grant row that names a team needs every permission of its role at its anchor. A write to a member row needs every permission of its role at the team.                                               |
| Delegation    | A grant to a team gives its role to every caller added to the team later. A caller who can write rows of the member entity decides who receives it.                                                                         |
| Delete        | A write that is not trusted cannot delete a team row while a live grant row names it as its subject. A trusted write can; the grant rows then grant nothing until a team row with that key exists again.                    |
| `caller_is`   | An access rule's `caller_is` can name a `string` member of a oneof. A row that holds another case reads as empty there, so the check does not match it.                                                                     |

### Registration rules

A schema **uses the access model** when any of its entities declares `access` or `grant`. An entity in such a schema that declares no `access` is **outside the model**: every caller the workspace gate admits reads and writes its rows, the application hooks still apply, and no rule, access parent or resync does. In a schema that declares a role, registration logs a warning for each such entity, because the gate admits only callers that hold a permission and the entity is open to all of them. A server built with `server.WithStrictAccess()` refuses such an entity instead.

Registration refuses a schema that uses the model when:

- An entity declares no `access` and the server is built with `WithStrictAccess`.
- An `access` declares no rule, or two rules with the same operation flags, source and checks.
- A rule's `when` condition does not compile against the entity, or uses a `rel` or `match` condition.
- A rule's `within` entry names no entity, sets neither `caller_is` nor `when`, repeats another entry of the rule, names an entity that is not in the model or whose rows the rule's rows cannot sit below (the rule's own entity counts only when its rows sit below rows of their own), names a `caller_is` that is not a top-level, singular `string` field of that entity, or has a `when` condition that does not compile against that entity or uses a `rel` or `match` condition.
- A rule sets no operation flag, sets neither `requires` nor `admitted`, sets both, or names one permission twice in `requires`.
- An entity marks an `access_parent` field that is a map, not a reference, nested inside a message field, or a workspace-qualified reference.
- A guard names no field, names a field that does not exist, a member of a oneof, a key field, an access parent, a `creator` field or a field another guard names.
- A guard has a rule that names `delete` or sets `fields`, two rules with the same operation flags, source and checks, or a rule whose `caller_is` or `when` reads a guarded field.
- A guard that can hide its fields names a field that a rule in `allow` checks with `caller_is` or `when`, a field that a `within` entry of any rule reads, or the subject or role field of a grant entity.
- A grant entity marks more than one `access_parent` field, other than the members of one oneof, or marks a repeated one.
- An entity marks some members of a oneof `access_parent` but not all of them.
- A case of an `access_parent` points at an entity that is not registered or declares no `access`.
- An entity can sit more `access_parent` hops below an entity without an access parent than `Limits.MaxAccessDepth` allows, through any case. A chain that comes back to an entity it passed, such as a folder inside a folder, has no such bound; its rows are bounded at write time instead (see [Access model](server.md#access-model)).
- A grant entity declares no `access`.
- A `grant` names a `subject` or `role` field that does not exist or has the wrong type, declares no roles, declares one role twice, or declares an enum role that is not a value of the enum.
- A `grant` subject is a repeated field, a workspace-qualified reference, or a reference to an entity that declares no `members`.
- An entity declares `members` without `access`, names no entity, or names an entity that is not registered, is not a grant entity, has an `access_parent` that does not point at the team entity alone, or has a subject that is not one `string` field.
- A role name is declared by two entities. Role names are unique across the schema.
- A role grants no permissions and includes no role, or grants one permission twice.
- A role includes a role that its own `grant` does not declare, includes one role twice, or includes itself, directly or through other roles.
- A permission that a rule's `requires`, a guard rule's `requires` or a `MutatorAccess` names is granted by no role.
- A permission or role name breaks the [naming rules](naming.md#permission-and-role-names).
- The `access`, rule, `admitted`, `within`, `guard`, `grant`, role, `members` or `access_parent` options carry fields the server does not know, which a newer `options.proto` can write.

`RegisterDescriptorSet` and `New` refuse a schema that breaks a rule. `RegisterEntity` adds one entity at a time, so a schema it builds can break a rule until its last entity is registered. While it does, every request to the server fails with `failed_precondition`.

Registration logs a warning when a rule never decides an operation, because another rule of the entity names it and admits every caller and row the first one admits. A rule that needs `edit_tasks` and `use_billing` never decides beside one that needs `edit_tasks` alone. It also logs a warning when a rule needs two permissions and every role that grants one grants the other: the rule then admits the same callers as a rule that needs the first alone.

Registration logs a warning when one permission governs entities at different depths that do not sit below one another, such as `view_members` on `OrgMember` (at the workspace root) and on `ProjectMember` (below a project). A role that grants such a permission at the workspace root reaches the rows below every anchor. One permission for a row and the rows below it, such as `view_tasks` on `Task` and on a `Comment` below a task, logs nothing.

Registration logs a warning when a role grants a permission that no rule and no `MutatorAccess` names. Only `EntityTx.Can` on the server and `hasPermission` on the client ask about such a permission, so the warning stays for a permission that only code checks. `EntityTx.Can` returns `server.ErrUnknownPermission` for a permission that no role grants.

### Changing the access model

A registration that changes the access model of an entity that is already registered is refused unless the caller allows it. A change is any change to the entity's `access` (its guards included), `grant`, `members` or `access_parent` options, or to the numbers of the enum values its role field matches, which includes adding `access` to an entity that had none or removing it. A change to the order of the rules is not a change, and neither is a change to `includes` that leaves each role with the same permissions. Allow it with `server.AllowAccessChange()` on `RegisterDescriptorSet`, `RegisterEntity`, `RegisterDynamicEntity` and `Manager.Register`, `allow_access_change` on `AdminService.Register`, or `allowAccessChange` on the TypeScript `registerSchema`.

A registration that declares the first role of a schema that served callers without one, or removes its last role, is refused the same way, even when no entity that was registered before changes. This includes a schema that did not use the access model before. A schema without a role admits every caller that `CanAccessWorkspace` lets in, and a schema with one admits only a caller that holds a permission, so the change decides who enters every workspace. The first registration of a server that holds no entity yet needs no flag. A run of `RegisterEntity` calls that leaves the model incomplete for a time is compared with the last schema that served callers.

Registration does not read stored rows. A stored grant row whose role the schema does not declare grants nothing, but keeps its value. When a registration declares that role, or adds `grant` to an entity that already holds rows, every stored row with that role value grants it. `New` has no earlier schema to compare with, so a schema compiled into the server applies such a change without `AllowAccessChange`.

Clients follow a change on their next pull. A change to who the read rules of an entity admit, which is the permissions of its read rules and whether a read rule uses `admitted`, resets every client that holds a cursor. So does a change to its `access_parent` field or its cases, and an entity that joins or leaves the model. Every other change, such as a role that grants other permissions, directly or through a role it includes, sends only the rows whose visibility changed. A change to the rules for create, update or delete changes no visibility. See [Access model](server.md#access-model) for every case.

## Secondary indexes

Secondary indexes can be declared on individual fields or as composite indexes on the entity. A field annotation desugars into a single-component index.

Mark a field:

```proto
string state = 8 [(syncplane.v1.field_behavior) = { indexed: true }];
```

Declare a composite index on the entity:

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

Selection criteria:

- **Field annotation (`indexed: true`)**: Use for single-field filters or sorting.
- **Composite index (`index`)**: Use when queries filter by one field and sort by another. A composite index satisfies the query in O(limit) time without scanning and filtering candidate keys.

`IndexSpec.name` must be unique within the entity and must not begin with `__` (reserved for internal indexes: `__ref_<field>` for references and `__field_<field>` for field annotations).

`IndexKey.path` is a dotted Protobuf path. A path targeting a single-key reference automatically resolves to that reference's key field. Setting `desc: true` sorts that component in descending order; missing and `NaN` values sort last regardless of direction.

A path can traverse repeated fields or target repeated scalars (`assignees.id`, `labels`). This produces a fan-out index where each row generates one index entry per element:

- Rows with an empty list generate a single entry tagged as missing, allowing unconstrained queries to locate all rows.
- An index can fan out over at most **one** repeated list. Multiple list fan-outs require cross products and are rejected at registration.
- A fan-out component provides ordering only when preceded by equality constraints on earlier index components.

Paths can also fan out over maps using `*` syntax. See [Maps and user-defined fields](#maps-and-user-defined-fields).

The following types cannot be indexed and are rejected at registration:

- Collaborative-text fields (use [Full-text search](#full-text-search)).
- Repeated message fields without a specified sub-field path.
- Maps using `indexed: true` (use composite `*` path syntax instead).

Secondary indexes are transparent and additive.

### Unsupported index shapes

Secondary indexes do not support the following query shapes:

| Shape                     | Reason                                                                                 |
| ------------------------- | -------------------------------------------------------------------------------------- |
| `contains` / `endsWith`   | Neither operator is a prefix. Use [Search](../how-to/search.md) for full-text search.  |
| `or`                      | Requires merging multiple ranges and falls back to a scan.                             |
| Collaborative-text fields | Writes would rewrite index entries on every keystroke. Rejected during registration.   |
| Maps or message lists     | Queries cannot compare nested structures. Index a scalar field on the element instead. |
| Ordering by a list        | List elements have multiple values, so rows lack a single sort position.               |

### Index lifecycle and write costs

- **Index availability**: A newly declared index is not available immediately. A background process indexes existing records first. Queries perform table scans until indexing finishes. On the client, the `indexing` event fires after bootstrap when indexing completes (see [Telemetry](telemetry.md#local-reads)).
- **Query results**: Using an index produces the same results as a full scan.
- **Local queries**: Local indexes cover confirmed rows for workspace-scoped queries. The engine merges the optimistic overlay over the range.
- **Write costs**: Each write updates the row's position in all defined indexes on both client and server. For repeated fields, the engine creates one entry per element, bounded by `MaxIndexEntriesPerRow`. Writes exceeding this limit are rejected with `invalid_argument`.
- **Value length**: One index entry holds the indexed values of the row and its key, and is at most 1536 bytes (`storage.MaxIndexEntryBytes`) on every store. A write whose entry would be longer is rejected with `invalid_argument`, so index a long string, such as a URL or a token, only when its values stay short. An index declared over rows that already hold longer values never becomes ready, and queries on it scan.
- **Diagnostics**: The `onSyncEvent` callback reports a `query` event with the execution `path` (`index` or `scan`), the active index name, and any decline reason. For details on decline reasons, see [Telemetry](telemetry.md#local-reads).

## Full-text search

Full-text search can be configured with field annotations, an entity-level `search` block, or both:

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

| Option                      | Description                                                                                                          |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `field_behavior.searchable` | Marks a field as searchable. Annotated fields take highest ranking precedence, evaluated in field declaration order. |
| `search.field[].path`       | Dotted Protobuf path to a singular string or collaborative-text field, in descending order of ranking precedence.    |
| `search.keep_diacritics`    | When `true`, preserves accents (for example, `café` and `cafe` remain distinct terms). Default is `false` (folded).  |

The `match` query condition processes configured fields through the search analyzer (lowercasing, accent folding unless disabled, and CJK bigram segmentation). The `relevance` rank orders matches by field precedence. Collaborative-text fields are indexed using their visible plain text.

Registration rejects:

- Paths to repeated fields, numbers, maps, or messages.
- `searchable: true` on unsupported field types.
- A `search` configuration with no searchable fields declared.
- Duplicate field paths across annotations and the `search` block.

Declaring search configures an inverted term index maintained on write and backfilled in the background. Rows exceeding 256 distinct terms store an overflow marker and are evaluated during search execution. For details on matching, ranking, and analyzer behavior, see [Search model](../explanation/search.md) and [Search data](../how-to/search.md).

## Merge behavior by field kind

| Field kind        | On `PATCH` with field in mask                                       | On `PUT`                                        |
| ----------------- | ------------------------------------------------------------------- | ----------------------------------------------- |
| Scalar, message   | Last-writer-wins for the field                                      | Replaced                                        |
| Map               | Merged by map key (mask can target one key)                         | Replaced                                        |
| Keyed list        | Merged by element key                                               | Replaced                                        |
| `opaque`          | Replaced as a whole or via custom merger                            | Replaced                                        |
| `text`            | Unioned with stored document                                        | **Unioned** with stored document (not replaced) |
| `oneof`           | Entire group replaced (mask targets group)                          | Replaced                                        |
| Reference message | Replaced as a whole (a sub-path into it replaces the whole message) | Replaced                                        |

Collaborative-text fields (`text: true`) are the only exception to whole-row replacement on `PUT`. See [Limits and trade-offs](../explanation/limits-and-trade-offs.md#put-replaces-the-whole-row).

## Oneofs

A oneof is treated as a single field. Field masks target the oneof group name (`choice`), not individual variant names (`choice_text`). A write replaces the entire group with the active variant or clears it if unset. Client helpers derive this mask automatically: `patch(schema, key, { choice: { case: 'choiceText', value: 'x' } })` and `update(key, draft => …)` emit the group path. See [Convergence model](../explanation/convergence-model.md#a-oneof-is-one-field).

Queries, orderings, and `indexed` annotations can reference individual variants directly. An unset variant evaluates to null or false:

```ts
const withValue = { where: (w) => w.choice.choiceText.eq('hit') }; // value of one case
const anyText = { where: (w) => w.choice.choiceText.isSet() }; // which case is active
```

### What a oneof cannot hold

Because oneofs replace as a single unit, schema registration rejects the following definitions inside a oneof:

| Type inside a oneof                   | Reason rejected                                                   |
| ------------------------------------- | ----------------------------------------------------------------- |
| Entity `key_field`                    | The row key would change when another variant is written.         |
| List `key_field` or `order_field`     | List elements would collide on zero values when variants switch.  |
| `text` field (directly or nested)     | The group replaces as a whole, preventing CRDT merge execution.   |
| Field with a registered custom merger | The merger cannot execute on paths bypassed by group replacement. |

References are allowed inside a oneof: a stored stub is replaced as a whole, and a oneof of references forms a [polymorphic reference](#polymorphic-references).

## Maps and user-defined fields

Maps merge per entry by map key. Individual entries are addressable in field masks, query conditions, and sort clauses:

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

// Application-defined value type. The engine does not interpret it.
message FieldValue {
	oneof kind {
		string text = 1;
		double number = 2;
		bool checked = 3;
		// Repeated scalars can be wrapped in a message.
		StringList labels = 4;
	}
}
```

### Addressing one entry

The path segment following a map field is the map entry key:

```
custom.priority.number      The `number` variant of the entry keyed "priority"
custom.priority             The entire entry (checked with isSet or isNotSet)
custom                      The entire map (checked with isEmpty)
```

Keys containing dots must be escaped with a backslash in string path representations (for example, `custom.due\.date.text`). Wire representations transmit path segments as discrete elements and require no escaping.

A key of an integral map is a decimal number inside the range of the key type. A signed key type accepts a leading `+` or `-`, and an unsigned key type accepts no sign. Leading zeros are allowed, so `by_id.007` and `by_id.+7` address the same entry as `by_id.7`. A `bool` key is `true` or `false`. Any other segment, and a number outside the range, is rejected at compile time.

In a field mask, the empty key is the empty segment. `custom.` names the entry with the empty key, and `custom` names the whole map. The same rule applies to a keyed-list element and to a scalar-set element whose key is the empty string, so a client that edits only that item leaves the other items to concurrent writers. `custom..text` descends into the entry with the empty key.

Queries reference the specific oneof variant intended. If an entry holds a different variant, the condition evaluates to false and sort clauses rank the entry last.

Sorting across different variants of a custom field is not supported (`order by custom.priority.number` is valid; `order by custom.priority` is not).

Comparing an entire map or an entire message-valued map entry in query predicates is rejected at compile time. Maps have implicit presence and do not support `isSet`; use `isEmpty` to check whether a map contains entries.

### Searching across keys: `any()`

To search across all map entries, use `any()`:

```go
query.Any("custom", query.Eq("value.number", 5))
query.Any("custom", query.And(
	query.StartsWith("key", "cf_"), query.Eq("value.checked", true)))
query.Any("custom", query.In("key", "priority", "severity"))
```

```ts
w.custom.any((e) => e.value.kind.number.eq(5));
w.custom.any((e) =>
	and(e.key.startsWith('cf_'), e.value.kind.checked.eq(true))
);
```

Within an `any()` predicate, `key` addresses the map key and `value` addresses the entry value. `any()` evaluates as a boolean predicate and does not return the matching key names.

### Indexing every entry: `*`

The `*` segment in an `IndexKey.path` fans out over every entry in the map. The subsequent segment (`key` or `value`) specifies the indexed component:

```proto
// One custom field, addressed by key: equality, ranges, and ordering.
index {
	name: "cf_number"
	key: [ { path: "custom.*.key" }, { path: "custom.*.value.number" } ]
}
// The same with an additional sort key.
index {
	name: "cf_num_recent"
	key: [
		{ path: "custom.*.key" },
		{ path: "custom.*.value.number" },
		{ path: "updated_at" desc: true }
	]
}
// Cross-key or named-key equality queries.
index {
	name: "cf_by_number"
	key: [ { path: "custom.*.value.number" }, { path: "custom.*.key" } ]
}
// Multi-select entries (one index entry per key-element pair).
index {
	name: "cf_labels"
	key: [ { path: "custom.*.key" }, { path: "custom.*.value.labels.values" } ]
}
```

Layout selection:

| Layout         | Supported operations                                                      |
| -------------- | ------------------------------------------------------------------------- |
| `[key, value]` | Equality, ranges, and sorting within a single named custom field.         |
| `[value, key]` | Equality queries across all custom fields or for a specific custom field. |

An application requiring only equality probes can declare a single `[value, key]` index to satisfy both access patterns. Declaring both layouts doubles index entries per row; see [Limits and trade-offs](../explanation/limits-and-trade-offs.md).

Indexing properties:

- A `*` component can be followed by a repeated field inside an entry (`custom.*.value.labels.values`), generating one index entry per `(key, element)` pair.
- When an entry key is fixed by an equality predicate, the value component can serve as an index sort key.
- Unconstrained sort operations on custom fields fall back to table scans because rows missing the key must sort last.
- A value component declared without a preceding key component supports cross-key `any()` queries, but single-key equality checks remain in the residual predicate filter.
- Setting `indexed: true` on a map field is rejected; use composite `*` index definitions instead.

### Admin UI limitations

The admin UI displays and filters scalar-valued maps only. Message-valued maps are omitted from the row view, edit form, and filter bar.

## Custom mergers for opaque fields

The server `WithMerger` option and client `MergerSpec` register a merge function for an opaque field. Merge functions must be idempotent, commutative, and associative. The merge function executes during server apply operations, optimistic overlay merges, and rebase replays. Non-idempotent mergers cause data corruption.

For collaborative text, use `text: true` instead of a custom merger; its built-in union merger satisfies these properties.

## Option field numbers

The custom option field numbers (`50100` for `entity`, `50101` for `field_behavior`, and `50102` for `reference`) reside in the Protobuf extension range reserved for internal organization use. If two imported Protobuf libraries claim the same extension number, `protobuf-go` panics at initialization with:

```
extension number 50100 is already registered on message google.protobuf.MessageOptions
```

Register extension numbers in the global Protobuf extension registry before publishing Protobuf definitions publicly.
