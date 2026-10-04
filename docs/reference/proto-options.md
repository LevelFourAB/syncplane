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

| Field           | Description                                                                                         |
| --------------- | --------------------------------------------------------------------------------------------------- |
| `name`          | Logical entity name used on the wire and in storage (for example, `"todo"`).                        |
| `key_fields`    | Field names, in order, whose values form the row key. Must be singular scalar fields.               |
| `index`         | Composite secondary indexes over this entity's fields. See [Secondary indexes](#secondary-indexes). |
| `create_policy` | Determines who may create rows of this entity. See [Write policies](#write-policies).               |
| `update_policy` | Determines who may modify existing rows of this entity. See [Write policies](#write-policies).      |
| `delete_policy` | Determines who may delete rows of this entity. See [Write policies](#write-policies).               |
| `server_only`   | When `true`, rows never sync to clients. See [Server-only entities](#server-only-entities).         |
| `history`       | Configures append-only field history tracking. See [Field history](#field-history).                 |
| `access`        | Names the permissions that govern rows of this entity. See [Access model](#access-model).           |
| `grant`         | Declares the rows of this entity as grant rows. See [Access model](#access-model).                  |

The `name` field must be a valid identifier. Each entry in `key_fields` must reference a singular string, integer, boolean, or enum field. For complete syntax and constraints, see [Naming rules](naming.md).

A message configured with `(syncplane.v1.entity)` is an **entity**. Message-typed fields that reference this message type automatically become entity references without additional annotations. See [References](#references) for what a reference stores and how it resolves.

<a id="delete-policy"></a>

### Write policies

Three options control write access to an entity's rows: `create_policy`, `update_policy`, and `delete_policy`. By default, each option is unset, which permits any caller allowed by the application's permission policy.

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

Setting a policy to `*_TRUSTED_ONLY` rejects that class of write for all regular callers with `permission_denied`. This restriction includes in-process API calls such as `Apply` and `Mutate` from an application handler. It permits only writes executed under the `Trusted` bypass, such as a connector, a migration, or a background job.

The engine evaluates these policies before running the application's `Permissions.CanMutate` hook on every write path.

#### Which policy a write must satisfy

The engine checks policies before opening the write transaction without reading the existing row. The check classifies the operation based solely on the incoming write request. An absent or tombstoned row has version 0, so `expected_version: 0` indicates that the operation applies only if the row does not exist.

| Write                                  | Policy required                           |
| -------------------------------------- | ----------------------------------------- |
| `PATCH`                                | `update_policy` (a `PATCH` never creates) |
| `DELETE`                               | `delete_policy`                           |
| `PUT` with `expected_version: 0`       | `create_policy`                           |
| `PUT` with non-zero `expected_version` | `update_policy`                           |
| `PUT` with no `expected_version`       | Both `create_policy` and `update_policy`  |

An unguarded `PUT` can create or update a row. The engine permits an unguarded `PUT` only when both `create_policy` and `update_policy` allow the caller. To write to an entity that restricts updates, a client must supply `expected_version: 0` (in Go, using `server.WithExpectedVersion(0)` on `Put`).

The TypeScript client evaluates this table locally and throws an error at the call site to prevent invalid writes from entering the pending queue.

#### Policy scope and limitations

Write policies apply to caller classes (trusted bypass versus authorized callers) rather than individual rows. Row-level permissions, such as restricting edits to the author of a comment, cannot be configured here. Implement row-level checks in `CanMutate` when ownership is derivable from the row key, or in a mutator body using `EntityTx.GetMeta` when the check requires reading the stored row. See [Check ownership in a mutator](../how-to/authentication-and-permissions.md#check-ownership-in-a-mutator).

Write policies apply to entire rows rather than individual fields. To restrict access to a specific field on a writable row, use [Protected fields](#protected-fields).

Write policies govern write primitives rather than high-level operations. Custom mutators are authorized at the invocation level rather than per individual row operation. An application requiring client-initiated modifications to a protected entity can implement a mutator that handles validation and cascading operations:

```go
server.WithMutator("archiveProject", &appv1.ArchiveArgs{},
	func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
		// … whatever must happen first …
		return tx.Delete(&appv1.Project{}, args.(*appv1.ArchiveArgs).GetId())
	})
```

Additional constraints:

- Batch operations apply sequentially in a single savepoint. A `DELETE(k)` followed by `PUT(k, expected_version: 0)` is evaluated as a create and executed as a replace. To prevent row replacement, configure `delete_policy` alongside `update_policy`.
- Collaborative-text fields are modified using `PATCH`. Clients can edit collaborative-text fields only when `update_policy` permits updates.
- Set `delete_policy` on rows with cascading dependencies to prevent unintended deletions.

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

Server-only entities are queryable but not syncable. Server-side `ViewTx` and mutators can access the entity, and clients can query it using the `Query` RPC (`queryServer` in TypeScript). Any client with workspace access can read these rows unless restricted by a [`ReadFilter`](../how-to/authentication-and-permissions.md#filtering-reads-and-writes-with-canmutate-and-readfilter). Setting `server_only: true` removes the entity from synchronization streams but does not restrict query access.

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
| `write_policy`     | `enum`     | Set to `FIELD_WRITE_POLICY_TRUSTED_ONLY` to restrict field modification to trusted callers while leaving the rest of the row writable. Allowed on top-level fields only. Protects a oneof as a group. Cannot be applied to key fields. See [Protected fields](#protected-fields).                                                                                                                                                                                                                                                                                                   |
| `workspace`        | `bool`     | Inside a reference message, marks the string field that names the workspace the referenced row lives in. Empty means the holder's own workspace. Valid on one singular `string` field of a message carrying `(syncplane.v1.reference)`. See [References](#references).                                                                                                                                                                                                                                                                                                              |
| `reverse_relation` | `string`   | Names the reverse relation this reference adds to its target entity. Empty means `<plural(holder entity)>By<Field>`, such as `issuesByAssignee`. Valid on a reference field declared on an entity, including a member of a oneof. See [Reverse relations](#reverse-relations).                                                                                                                                                                                                                                                                                                      |
| `access_parent`    | `message`  | Places each row below the row this reference points at in the access model. Set it as `access_parent: {}`. On a oneof, mark every member. See [Access model](#access-model).                                                                                                                                                                                                                                                                                                                                                                                                        |

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

## Protected fields

The entity options `create_policy`, `update_policy`, and `delete_policy` govern entire rows. The `write_policy` field option restricts write access to an individual field, such as a server-maintained status, counter, or immutable creator field:

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

Protected fields handle write operations as follows:

| Write                                | Outcome                                                                             |
| ------------------------------------ | ----------------------------------------------------------------------------------- |
| `PATCH` whose mask targets the field | Rejected with `permission_denied`.                                                  |
| `PUT`                                | Applied; the protected field retains its stored value (or zero value on a new row). |
| `DELETE`                             | Applied without restriction (field protection guards the value, not the row).       |

A `PATCH` explicitly specifies its target fields, so targeting a protected field fails immediately. A `PUT` sends the entire row, so the engine preserves the stored value of protected fields to allow read-modify-write workflows when unrelated fields change.

Field masks match against the **first path segment**: protecting `audit` also protects `audit.size`. A oneof is protected as a unit; marking any member protects all variants in the oneof. Key fields cannot be marked with `write_policy` and fail schema registration.

Clients cannot edit a protected collaborative-text field: collaborative edits require `PATCH`, and a `PUT` replaces the document with the stored server copy rather than merging client edits.

The TypeScript client validates protected fields locally: `patch()` calls targeting a protected field throw an error, and `put()` calls substitute the local cached value into the optimistic overlay.

For caller-dependent rules (such as allowing owners to update a field while restricting viewers), configure the server's `FieldPermissions` hook. See [Authentication and permissions](../how-to/authentication-and-permissions.md#protecting-fields-per-caller).

Mutator operations are authorized at the invocation level. A mutator can update protected fields when invoked by an authorized caller.

## Access model

Three options declare who can read and write rows: `access` and `grant` on an entity, and `access_parent` on a reference field.

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
				{ name: "org_admin" grants: [ "view_members", "manage_members", "view_projects", "edit_projects", "view_tasks", "edit_tasks" ] }
			]
		}
		access: { read: "view_members" write: "manage_members" }
	};
	string id = 1;
	string user_id = 2;
	string role = 3;
}

message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		access: { read: "view_projects" write: "edit_projects" }
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
		access: { read: "view_members" write: "manage_members" }
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
		access: { read: "view_tasks" write: "edit_tasks" }
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
}

// Outside the model: every caller admitted to the workspace reads and writes it.
message Label {
	option (syncplane.v1.entity) = {
		name: "label"
		key_fields: [ "id" ]
		access: { unrestricted: true }
	};
	string id = 1;
}
```

A caller holds a permission at an **anchor**: the workspace root, or one row that a grant row's `access_parent` field points at. A permission held at an anchor applies to the anchor row and to every row below it: the rows whose `access_parent` field points at it, the rows whose `access_parent` field points at those, and so on. A comment whose parent is a task, whose parent is a project, is below the project. An entity can be its own access parent, such as a folder inside a folder.

| Option                         | Field          | Description                                                                                                                                                                                                          |
| ------------------------------ | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `access`                       | `read`         | The permission that lets a caller read rows. Required unless `unrestricted` is set.                                                                                                                                  |
| `access`                       | `write`        | The permission that lets a caller create, update and delete rows. Required unless `unrestricted` is set.                                                                                                             |
| `access`                       | `unrestricted` | Puts the entity outside the model: every caller admitted to the workspace reads and writes its rows, subject to the application permission hooks. Cannot be combined with `read` or `write`.                         |
| `grant`                        | `subject`      | The name of the field that holds the subject the row grants to: a top-level, singular `string` field outside any oneof.                                                                                              |
| `grant`                        | `role`         | The name of the field that holds the role the row grants: a top-level, singular `string` or enum field outside any oneof. An enum value names the role with the same name as the value.                              |
| `grant`                        | `roles`        | The roles rows can grant. Each role has a `name` and the permissions it `grants`.                                                                                                                                    |
| `field_behavior.access_parent` | (none)         | Marks the reference that places each row below another row, which can have an access parent of its own. A grant row anchors at the row it points at. A grant entity without the field anchors at the workspace root. |

A grant row grants nothing when its subject is empty, its role is not declared in `roles`, or its `access_parent` field is unset or names a row that does not exist. Every write path refuses to store a grant row with an empty subject, an undeclared role, or no `access_parent` on an entity that has one.

A caller can create a row where it holds the entity's `write` permission, and update or delete a row where the stored row sits. A write that moves a row to another access parent needs `write` at both, and the `read` permission of every entity whose rows can sit below it where the row is before the move. A delete of a row that a live row names as its access parent is refused. A write that can change what a grant row grants needs every permission of the role at the row's anchor, before and after the write, so a caller grants and revokes only what it holds itself. See [Access model](server.md#access-model) for the full write rules and for mutators.

### Polymorphic access parents

An access parent can point at one of several entities. Declare it in either shape of a [polymorphic reference](#polymorphic-references): mark every member of a oneof on the entity, or mark a field whose reference message holds a oneof of entities.

```proto
// A comment on a task, or on a doc.
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: { read: "view_comments" write: "edit_comments" }
	};
	string id = 1;
	oneof subject {
		Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
		Doc doc = 3 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	}
}
```

| Rule        | Description                                                                                                                                                                                                                     |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Parent row  | The case a row holds names the row it sits below. A row that holds no case, or holds a case that names no key, has no parent.                                                                                                   |
| Permissions | The entity's `read` and `write` permissions apply in every case. A role reaches the rows below a target only when it grants them, so grant `view_comments` both in the roles that reach tasks and in the roles that reach docs. |
| Cases       | Every member of the oneof is marked, and every member is a reference. A reference message with a `workspace` field is refused.                                                                                                  |
| Moves       | A write that changes the case or the key is a move, with the write rules above. A task and a doc with the same key are two rows.                                                                                                |
| Masks       | A `PATCH` reaches the access parent when its mask names the oneof group (`subject`), or the field that holds the reference message.                                                                                             |
| Depth       | `Limits.MaxAccessDepth` applies to the deepest chain of any case.                                                                                                                                                               |

### Registration rules

A schema **uses the access model** when any of its entities declares `access` or `grant`. Registration refuses such a schema when:

- An entity declares no `access`. Declare `access: { unrestricted: true }` for an entity outside the model.
- An `access` sets `unrestricted` together with a permission, or names only one of `read` and `write`.
- An entity marks more than one `access_parent` field, other than the members of one oneof, or marks one that is repeated, not a reference, nested inside a message field, a workspace-qualified reference, or protected by `write_policy`.
- An entity marks some members of a oneof `access_parent` but not all of them.
- A case of an `access_parent` points at an entity that is not registered or is outside the model.
- An entity can sit more `access_parent` hops below an entity without an access parent than `Limits.MaxAccessDepth` allows, through any case. A chain that comes back to an entity it passed, such as a folder inside a folder, has no such bound; its rows are bounded at write time instead (see [Access model](server.md#access-model)).
- A grant entity does not declare `access` with `read` and `write`.
- A `grant` names a `subject` or `role` field that does not exist or has the wrong type, declares no roles, declares one role twice, or declares an enum role that is not a value of the enum.
- A role name is declared by two entities. Role names are unique across the schema.
- A role grants no permissions, or one permission twice.
- A permission that an entity names is granted by no role, or a role grants a permission that no entity names.
- A permission or role name breaks the [naming rules](naming.md#permission-and-role-names).
- The `access`, `grant`, role or `access_parent` options carry fields the server does not know, which a newer `options.proto` can write.

`RegisterDescriptorSet` and `New` refuse a schema that breaks a rule. `RegisterEntity` adds one entity at a time, so a schema it builds can break a rule until its last entity is registered. While it does, every request to the server fails with `failed_precondition`.

Registration logs a warning when one permission governs entities at different depths that do not sit below one another, such as `view_members` on `OrgMember` (at the workspace root) and on `ProjectMember` (below a project). A role that grants such a permission at the workspace root reaches the rows below every anchor. One permission for a row and the rows below it, such as `view_tasks` on `Task` and on a `Comment` below a task, logs nothing.

### Changing the access model

A registration that changes the access model of an entity that is already registered is refused unless the caller allows it. A change is any change to the entity's `access`, `grant` or `access_parent` options, or to the numbers of the enum values its role field matches. Allow it with `server.AllowAccessChange()` on `RegisterDescriptorSet` and `Manager.Register`, `allow_access_change` on `AdminService.Register`, or `allowAccessChange` on the TypeScript `registerSchema`.

Registration does not read stored rows. A stored grant row whose role the schema does not declare grants nothing, but keeps its value. When a registration declares that role, or adds `grant` to an entity that already holds rows, every stored row with that role value grants it. `New` has no earlier schema to compare with, so a schema compiled into the server applies such a change without `AllowAccessChange`.

Clients follow a change on their next pull. A change to an entity's `read` permission, `unrestricted` setting, `access_parent` field or its cases resets every client that holds a cursor. Every other change, such as a role that grants other permissions, sends only the rows whose visibility changed. See [Access model](server.md#access-model) for every case.

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
