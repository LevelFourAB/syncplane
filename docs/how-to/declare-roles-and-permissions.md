# Declare roles and permissions

This guide sets up the access model for an app where an organization has projects, projects have members, and tasks belong to projects. Org admins see and edit everything, and a project member sees only that project. For how the model works, see [The access model](../explanation/access-model.md).

## 1. Choose the permissions

Start with one permission to read the rows of each entity and one to write them. A rule with `write` admits create, update and delete. To give these operations to different callers, see [Allow some operations and not others](#allow-some-operations-and-not-others). Use separate names for entities at different levels, so a role granted on the whole workspace does not reach more than you intend:

| Entity           | Read            | Write            |
| ---------------- | --------------- | ---------------- |
| `org_member`     | `view_members`  | `manage_members` |
| `project`        | `view_projects` | `edit_projects`  |
| `project_member` | `view_roster`   | `manage_roster`  |
| `task`           | `view_tasks`    | `edit_tasks`     |

## 2. Declare the grant entities

A grant entity holds one row per subject and role. Its rows apply at the row its access parent points at, or at the workspace root when its `access` names no `parents`.

```proto
message OrgMember {
	option (syncplane.v1.entity) = {
		name: "org_member"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [
				{ name: "org_member" grants: [ "view_projects", "view_tasks" ] },
				{ name: "org_admin" grants: [ "view_members", "manage_members", "view_projects", "edit_projects", "view_roster", "manage_roster", "view_tasks", "edit_tasks" ] }
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

message ProjectMember {
	option (syncplane.v1.entity) = {
		name: "project_member"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [
				{ name: "project_reader" grants: [ "view_projects", "view_roster", "view_tasks" ] },
				{ name: "project_admin" includes: [ "project_reader" ] grants: [ "manage_roster", "edit_tasks" ] }
			]
		}
		access: {
			parents: [ "project" ]
			allow: [
				{ read: true requires: "view_roster" },
				{ write: true requires: "manage_roster" }
			]
		}
	};
	string id = 1;
	Project project = 2;
	string user_id = 3;
	string role = 4;
}
```

`project_admin` includes `project_reader`, so it grants every permission of `project_reader` and the two it lists. When you add a permission to `project_reader`, `project_admin` grants it too. A role can include only roles of its own `grant`, and a role can include a role that includes other roles. Registration refuses a role that includes itself through any chain.

Role names are unique across the schema. A role granted to a caller can only be given or taken away by a caller who holds all of its permissions at the same row.

Every write refuses a role that the schema does not declare, but registration does not check rows that are already stored. When you add `grant` to an entity that already holds rows, or add a role, each stored row whose role value matches a declared role starts to grant it. Check the stored values before you deploy such a change.

### Give a role to a team

First choose how your app uses teams:

| Your app needs                                                     | Use                                                                                                           |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| Each project belongs to one team                                   | Name `team` in the `parents` of `Project`, and grant roles on the team.                                       |
| Teams come from your identity provider                             | Return the caller's groups from `WithCallerAccess`, such as `group:eng`, and grant to them.                   |
| A team gets a role on projects that other teams or users also have | Team grants, below.                                                                                           |
| A project is open to the whole workspace or to chosen teams        | [Share a project with the whole workspace or with chosen teams](share-with-the-workspace-or-chosen-teams.md). |

For team grants, declare who the members of a team are with `members` on the team entity. Each `team_member` row makes its user a member of its team, whatever the role:

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
	string name = 2;
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
			parents: [ "team" ]
			allow: [
				{ read: true requires: "view_teams" },
				{ write: true requires: "manage_teams" }
			]
		}
	};
	string id = 1;
	Team team = 2;
	string user_id = 3;
	string role = 4;
}
```

Then let `ProjectMember` name a user or a team. Make its subject a oneof, and keep the roles you have. `user_id` and `role` keep their field numbers, and `team` gets a new one, so the rows you stored before still decode:

```proto
message ProjectMember {
	option (syncplane.v1.entity) = {
		name: "project_member"
		key_fields: [ "id" ]
		grant: {
			subject: "member"
			role: "role"
			roles: [
				{ name: "project_reader" grants: [ "view_projects", "view_roster", "view_tasks" ] },
				{ name: "project_admin" includes: [ "project_reader" ] grants: [ "manage_roster", "edit_tasks" ] }
			]
		}
		access: {
			parents: [ "project" ]
			allow: [
				{ read: true requires: "view_roster" },
				{ write: true requires: "manage_roster" }
			]
		}
	};
	string id = 1;
	Project project = 2;
	oneof member {
		string user_id = 3;
		Team team = 5;
	}
	string role = 4;
}
```

A `ProjectMember` row with `team` set gives its role to every member of that team. When someone joins or leaves the team, their clients receive or lose the project's rows on the next pull. One roster holds users and teams, so the UI lists both from one entity.

Before you ship, check these points:

- **Who manages a team decides who gets its grants.** A grant to a team gives its role to every user added to the team later. Give `manage_teams` only to callers you trust with every grant the team holds.
- **Every role of the member entity makes a member.** For guests who see a team but do not get its grants, use a separate grant entity anchored at the team.
- **A team cannot be a member of a team.** Registration refuses a member entity whose subject is a team.
- **To give teams other roles than users**, declare a separate grant entity with a plain `Team team` subject and its own roles. Role names are unique across the schema, so name them differently, such as `project_team_reader`.
- **To let a user leave a project**, add `{ delete: true requires: "view_roster" caller_is: "user_id" }`. A team's row holds no `user_id`, so the rule does not let a member remove the team.
- **Delete the grants before the team.** A client cannot delete a team that a live grant row names.
- **Admins are members only with a row.** A role at the workspace root does not make a caller a member of a team. To let an admin see what a team sees, grant the admin the same permissions at the root.

## 3. Declare access on the entities the roles govern

An entity that declares no `access` stays open to every caller that enters the workspace, so declare `access` on each entity whose rows a role decides, and leave the others alone. Name the reference that places a row below another row in `parents`; the entity it points at must declare `access` too:

```proto
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
	string name = 2;
}

message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			parents: [ "project" ]
			allow: [
				{ read: true requires: "view_tasks" },
				{ write: true requires: "edit_tasks" }
			]
		}
	};
	string id = 1;
	Project project = 2;
	string title = 3;
}
```

A row can sit below a row that sits below another row. A comment below a task is below the task's project, so a role on the project reaches the comment. Give the comment the task's permissions when a caller who reads a task should read its comments:

```proto
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: {
			parents: [ "task" ]
			allow: [
				{ read: true requires: "view_tasks" },
				{ write: true requires: "edit_tasks" }
			]
		}
	};
	string id = 1;
	Task task = 2;
	string body = 3;
}
```

An entity can also be its own access parent, for folders inside folders. A grant row whose access parent is a folder gives its role on that folder and on everything below it:

```proto
message Folder {
	option (syncplane.v1.entity) = {
		name: "folder"
		key_fields: [ "id" ]
		access: {
			parents: [ "parent" ]
			allow: [
				{ read: true requires: "view_folders" },
				{ write: true requires: "edit_folders" }
			]
		}
	};
	string id = 1;
	Folder parent = 2;
}
```

A chain is at most `Limits.MaxAccessDepth` rows long (32 by default). The server refuses a folder moved below itself, or moved where it or a folder below it would sit deeper than that. A client that creates or moves a folder below another folder must read that folder.

A row can sit below one of several entities, such as a comment on a task or on a doc. Name a oneof of references in `parents`. The case a row holds decides where it sits:

```proto
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: {
			parents: [ "subject" ]
			allow: [
				{ read: true requires: "view_comments" },
				{ write: true requires: "edit_comments" }
			]
		}
	};
	string id = 1;
	oneof subject {
		Task task = 2;
		Doc doc = 3;
	}
	string body = 4;
}
```

The comment's rules apply in both cases. Add `view_comments` and `edit_comments` to the project roles and to the folder roles, so that a role in either tree reaches the comments below it. A reference message whose target is a oneof of entities works the same way: name the field that holds it. See [Polymorphic access parents](../reference/proto-options.md#polymorphic-access-parents).

Registration refuses a schema with a permission that no role grants, and logs a warning for a permission that a role grants but nothing uses. The error and the warning name the permission and the entity.

The TypeScript schema builder declares the same options with `access`, its `parents` included, and `grant`; see the [TypeScript client reference](../reference/typescript-client.md#access-model). The builder has no `oneof` and no enum fields, so a subject that is a user or a team, a polymorphic access parent and an enum role field need a proto schema.

### Allow some operations and not others

Each rule names the operations it admits. An operation is allowed when at least one rule admits it. An operation that no rule names is open only to trusted writes: code that runs under `server.Trusted`, connectors, effects and the bodies of custom mutators. The server refuses a client write that can only be such an operation before `CanMutate` runs, and the TypeScript client rejects it at the call site.

To give create, update and delete to different permissions, write one rule for each. Here a member who may comment creates comments, and a moderator changes and deletes them:

```proto
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: {
			parents: [ "task" ]
			allow: [
				{ read: true requires: "view_tasks" },
				{ create: true requires: "post_comments" },
				{ update: true delete: true requires: "moderate_comments" }
			]
		}
	};
	string id = 1;
	Task task = 2;
	string body = 3;
}
```

Add `post_comments` and `moderate_comments` to the roles that must hold them. A move to another task needs one update rule that admits the caller at both tasks, so here only a moderator moves a comment.

To make an entity server-owned, give it a read rule and no other rule. Clients read its rows, and only trusted writes create, change or delete them:

```proto
// The server records activity. Clients read it.
message Activity {
	option (syncplane.v1.entity) = {
		name: "activity"
		key_fields: [ "id" ]
		access: {
			parents: [ "task" ]
			allow: [ { read: true requires: "view_tasks" } ]
		}
	};
	string id = 1;
	Task task = 2;
	string what = 3;
}
```

Write its rows from trusted code, such as an effect or `srv.Mutate(server.Trusted(ctx), ...)`. See [Write from server-side code](server-side-writes.md).

To make an entity append-only, give it a read rule and a create rule. A client creates a row and cannot change or delete it after that:

```proto
message TaskNote {
	option (syncplane.v1.entity) = {
		name: "task_note"
		key_fields: [ "id" ]
		access: {
			parents: [ "task" ]
			allow: [
				{ read: true requires: "view_tasks" },
				{ create: true requires: "edit_tasks" }
			]
		}
	};
	string id = 1;
	Task task = 2;
	string body = 3;
}
```

A client creates a note with `put`. The server refuses a `put` of a note that exists, because that write is an update. The TypeScript client rejects `patch`, `update` and `del` on a note before it sends them.

To let every caller in the workspace read an entity, use `admitted: {}` as the source of the read rule. A caller is admitted when `CanAccessWorkspace` lets it in and, in a schema that declares a role, it holds at least one permission in the workspace:

```proto
// Every caller in the workspace reads labels. Org admins write them.
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
	string name = 2;
}
```

An entity whose rules use `admitted` stays in the model. It can have an access parent, be the access parent of other entities, and be a grant entity.

To keep a client from deleting rows in a schema that does not use the access model, refuse the delete in `CanMutate`. See [Reserve an operation for trusted writes](authentication-and-permissions.md#reserve-an-operation-for-trusted-writes).

### Make a row read-only, or hide it, by a field of the row

Add `when` to a rule to admit only rows that match a condition. The condition uses the query predicate language, with paths from the row. Here a task writer edits a task until someone archives it, a hidden task is read only by admins, and an admin archives and restores tasks:

```proto
message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			parents: [ "project" ]
			allow: [
				{
					read: true
					requires: "view_tasks"
					when: { cmp: { path: [ "hidden" ] op: CMP_OP_EQ value: { bool_value: false } } }
				},
				{ read: true requires: "manage_tasks" },
				{
					update: true
					requires: "edit_tasks"
					when: { cmp: { path: [ "archived" ] op: CMP_OP_EQ value: { bool_value: false } } }
				},
				{ write: true requires: "manage_tasks" }
			]
		}
	};
	string id = 1;
	Project project = 2;
	bool archived = 3;
	bool hidden = 4;
}
```

- The update rule must admit the stored row and the row as it will be stored, so a holder of `edit_tasks` cannot archive a task or restore one. Give that to another rule, here the `write` rule with `manage_tasks`.
- When a task becomes hidden, clients that read it through the first rule alone receive a delete. When it stops being hidden, they receive it again.
- To let the author read their own draft, add a second read rule with `caller_is` beside the one with `when`. The rules join with OR.
- A condition reads only the row. To decide by a field of the project above the task, use `within`; see the next section.

In the TypeScript schema builder, `when` takes the same field proxy as a query's `where`:

```ts
{ update: true, requires: 'edit_tasks', when: (t) => t.archived.eq(false) }
```

`can()` evaluates the condition on the row the client holds. It answers `'unknown'` for an update whose `fields` include a field the condition reads, because the new value decides.

### Decide by a field of a row above: private, archived and owned parents

Add `within` to a rule to admit only rows that sit within a row that passes a check. The entry names the entity of that row, and a `caller_is` field or a `when` condition on it. Here a private project hides its tasks and their comments from org members, an archived project makes its tasks read-only, and the owner of a project reads and writes the comments of its tasks:

1. Give the org role and the project role different read permissions, so that one rule can check the project for org members and another can leave it unchecked for project members:

   ```proto
   roles: [
     { name: "org_member" grants: [ "view_projects", "view_open_tasks" ] },
     ...
   ]
   ```

   ```proto
   roles: [
     { name: "project_member" grants: [ "view_projects", "view_tasks", "edit_tasks" ] },
     ...
   ]
   ```

2. Declare the fields on the parent. Mark the owner field `indexed`, so that a read by an owner who reads through the root narrows to the owner's rows:

   ```proto
   message Project {
   	option (syncplane.v1.entity) = {
   		name: "project"
   		key_fields: [ "id" ]
   		access: {
   			allow: [
   				{ read: true requires: "view_projects" },
   				{ update: true requires: "edit_projects" }
   			]
   		}
   	};
   	string id = 1;
   	bool private = 2;
   	bool archived = 3;
   	string owner_id = 4 [(syncplane.v1.field_behavior) = { indexed: true }];
   }
   ```

3. Add the rules with `within` to the entities below:

   ```proto
   message Task {
   	option (syncplane.v1.entity) = {
   		name: "task"
   		key_fields: [ "id" ]
   		access: {
   			parents: [ "project" ]
   			allow: [
   				{ read: true requires: "view_tasks" },
   				{
   					read: true
   					requires: "view_open_tasks"
   					within: {
   						entity: "project"
   						when: { cmp: { path: [ "private" ] op: CMP_OP_EQ value: { bool_value: false } } }
   					}
   				},
   				{
   					write: true
   					requires: "edit_tasks"
   					within: {
   						entity: "project"
   						when: { cmp: { path: [ "archived" ] op: CMP_OP_EQ value: { bool_value: false } } }
   					}
   				}
   			]
   		}
   	};
   	string id = 1;
   	Project project = 2;
   }

   message Comment {
   	option (syncplane.v1.entity) = {
   		name: "comment"
   		key_fields: [ "id" ]
   		access: {
   			parents: [ "task" ]
   			allow: [
   				{ read: true requires: "view_tasks" },
   				{
   					read: true
   					requires: "view_open_tasks"
   					within: {
   						entity: "project"
   						when: { cmp: { path: [ "private" ] op: CMP_OP_EQ value: { bool_value: false } } }
   					}
   				},
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
   	Task task = 2;
   }
   ```

- The entry holds when the row itself, or any row above it, is a row of the named entity that passes. The comment is two levels below the project, and the entry finds it.
- When a project becomes private, org members receive a delete for each of its tasks and comments on their next pull. When it stops being private, they receive them again. The same happens to the comments when the owner changes.
- A write that changes `private` or `owner_id` changes what other callers read below the project, so it needs, where the project is, a read rule of each entity below it. A caller that edits projects but cannot read their tasks cannot make a project public.
- In a tree of one entity, such as folders inside folders, one row that passes is enough. A private inner folder below a folder that is not private does not hide its documents through such a rule.

In the TypeScript schema builder, build each entry through `schema.within`, which types `callerIs` and `when` against the parent's fields. Pass a thunk, `() => project`, when `project` is declared after the entity:

```ts
{ read: true, requires: 'view_open_tasks', within: schema.within(() => project, { when: (p) => p.private.eq(false) }) },
{ read: true, write: true, admitted: true, within: schema.within(() => project, { callerIs: 'ownerId' }) },
```

In a tree of one entity, write `{ self: true, when }` for an entry on the entity's own rows. TypeScript types `when` against the entity's fields:

```ts
{ read: true, admitted: true, within: { self: true, when: (f) => f.private.eq(false) } },
```

`can()` reads the rows above the row from the rows the client holds. It answers `'unknown'` when a row on the way is not held and a row of the named entity can be that row or sit above it, and for an update that moves the row.

### Require two permissions together

Rules join with OR, so a caller that holds either of two rules' permissions passes. To require two permissions together, list both in one rule's `requires`. Here a project writer edits a task only when they also hold `use_billing`, which a billing role grants at the workspace root:

```proto
message Biller {
	option (syncplane.v1.entity) = {
		name: "biller"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [ { name: "billing_clerk" grants: [ "view_billers", "use_billing" ] } ]
		}
		access: { allow: [ { read: true requires: "view_billers" } ] }
	};
	string id = 1;
	string user_id = 2;
	string role = 3;
}

message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			parents: [ "project" ]
			allow: [
				{ read: true requires: "view_tasks" },
				{ write: true requires: [ "edit_tasks", "use_billing" ] }
			]
		}
	};
	string id = 1;
	Project project = 2;
}
```

- Each permission can be held at a different place on the row's chain: `edit_tasks` at the task's project and `use_billing` at the workspace root, or both at the project.
- A caller that holds only one of them cannot write the task.
- To move a task, the caller needs both permissions at the old project and at the new one.
- On a read rule, a caller reads only the rows where it holds every permission of the rule. When it loses one, its clients receive a delete for each row it can no longer read.

In the TypeScript schema builder, `requires` takes one name or a list:

```ts
{ write: true, requires: [ 'edit_tasks', 'use_billing' ] }
```

`can()` answers `'yes'` only when the client can place every permission of the rule on the row's chain.

### Let only some callers change a field

A rule in `allow` decides whole rows. To restrict who changes one field of a row that others edit, add a field guard. Here every task editor edits the title, only a caller with `approve_tasks` sets the status, and only server code sets the cost:

```proto
message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			parents: [ "project" ]
			allow: [
				{ read: true requires: "view_tasks" },
				{ write: true requires: "edit_tasks" }
			]
			guard: [
				{
					fields: [ "status" ]
					allow: [
						{ read: true admitted: {} },
						{ write: true requires: "approve_tasks" }
					]
				},
				{ fields: [ "cost" ] allow: [ { read: true admitted: {} } ] }
			]
		}
	};
	string id = 1;
	Project project = 2;
	string title = 3;
	string status = 4;
	int64 cost = 5;
}
```

- **A guarded write needs both rules.** An editor without `approve_tasks` changes the title, and a `PATCH` of the status is refused. An approver without `edit_tasks` changes nothing, because no rule in `allow` admits the update.
- **A `PUT` keeps what the caller may not write.** An editor who sends the whole row with a new title and a new status changes the title, and the status keeps its stored value.
- **Every reader reads the field.** Each guard declares `{ read: true admitted: {} }`. To keep a field from some readers, see the next section.
- **Use `fields` on a rule for the opposite case.** A rule `{ update: true requires: "view_tasks" caller_is: "assignee_id" fields: [ "status" ] }` lets an assignee change the status and nothing else, beside the editors' rule.

Add the guard permission to the `grants` of each role that gives it, as for a rule. On the client, `can(schema, key, 'update', { fields: [ 'status' ] })` predicts the guard. See [Field guards](../reference/proto-options.md#field-guards).

### Let only some callers read a field

Give the field's guard a read rule narrower than `{ read: true admitted: {} }`. Here every staff reader reads the name, only a caller with `hr` reads and sets the salary, only the person a record is about reads the review, and no client reads the bonus:

```proto
message Staff {
	option (syncplane.v1.entity) = {
		name: "staff"
		key_fields: [ "id" ]
		access: {
			parents: [ "project" ]
			allow: [
				{ read: true requires: "view_staff" },
				{ write: true requires: "edit_staff" }
			]
			guard: [
				{ fields: [ "salary" ] allow: [ { read: true write: true requires: "hr" } ] },
				{
					fields: [ "review" ]
					allow: [
						{ read: true admitted: {} caller_is: "person_id" },
						{ write: true requires: "hr" }
					]
				},
				{ fields: [ "bonus" ] }
			]
		}
	};
	string id = 1;
	Project project = 2;
	string name = 3;
	int64 salary = 4;
	string review = 5;
	string person_id = 6;
	int64 bonus = 7;
}
```

- **The client receives the field cleared.** A reader without `hr` receives each staff row with `salary` at its zero value, and `Row.hidden` lists `salary`. Show such a field as hidden, not as empty.
- **A query cannot name it.** A `queryServer` filter, order or facet on `salary`, or a subscription on it, fails with `PermissionDenied` for a caller that does not hold `hr` at the workspace root. A role held at a project shows the salary of that project's rows, and still does not let the caller query by it.
- **A grant change resends the rows.** Give a caller `hr` and the next pull sends the rows again with the salary in them.
- **A `PUT` keeps what the caller did not read.** An editor who writes back a whole row changes the name and keeps the stored salary, review and bonus.
- **A field that decides access stays readable.** Registration refuses to hide a field that a rule checks with `caller_is` or `when`, so `person_id` stays visible here.
- **Only a reader of the review changes who reads it.** A client write that changes `person_id` needs the right to read `review` before and after the write, so an editor cannot point a record at itself to read the review. To reassign a record, use a trusted write, such as a mutator body.

On the client, `can(schema, key, 'read', { fields: [ 'salary' ] })` predicts whether the caller reads the field. See [Who reads a guarded field](../reference/proto-options.md#who-reads-a-guarded-field).

### Put a row below several parents

Name each reference that places the row in `parents`, or name a repeated reference. Here a task sits below its project and below its milestone, and a milestone planner attaches tasks to milestones without the right to edit them:

```proto
message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: {
			parents: [ "project", "milestone" ]
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
	Project project = 2;
	Milestone milestone = 3;
}
```

- A reader of the project or of the milestone reads the task, and the rows below it.
- To create a task in a project and a milestone, the caller needs `edit_tasks` at both, or at the workspace root.
- To attach a task to a milestone, or to detach it, the caller needs the rule at that milestone. A planner who holds `plan_tasks` at the project and at both milestones moves a task between them.
- To edit the title or delete the task, the rule through one parent is enough.
- For a list, such as `repeated Space spaces`, add or remove one space with a `PATCH` whose mask names `spaces.<key>`. Two clients that add a space at the same time keep both.
- A grant entity keeps one parent. Write one grant row for each row where a role applies.

In the TypeScript schema builder, name each `ref()` or `refList()` field in `access.parents`, by accessor name. `canCreate()` answers `'yes'` only when the client can place the permission through each parent of the new row.

### Moving and deleting rows that other rows sit below

Rows below a row move with it. To move a task to another project, the caller needs one update rule that admits it at both projects, here the `write` rule with `edit_tasks`, and, at the old project, the right to read the task itself, each field of it a guard can hide, and every row of each entity that can sit below a task, here `view_tasks` for the task and the comments. A read rule with `caller_is` or `when`, such as "authors read their own comments", reads some rows only and does not count. Without these read rights a caller could carry rows and fields it cannot read to a project where it or other people can read them. A field or an entity whose read rules need no permission and set no `within` entry is left out of these checks: a field that no client reads, or comments that only their authors read, have the same readers at every project, so the move shows them to nobody new.

A client cannot delete a row while a live row names it as its access parent. Delete or move the comments first; one batch can delete the comments and the task together. Trusted code can delete the task anyway, and its comments then sit below nothing, so only a caller who holds `view_tasks` at the workspace root reads them.

A client cannot create a task with that key while those comments are live: the create is refused with `conflict`, so a new task does not take over the comments of the old one. The same applies to a row whose key a live grant row names. Delete or move the comments first, or create the row with trusted code, which puts the comments below it.

A client `PATCH` or `DELETE` of a row that does not exist is refused with `access_denied`, the same answer as for a row the caller may not change. A client that deletes a row that another client deleted first therefore sees the delete in `onMutationRejected`.

## 4. Tell the server who the caller is

Grant rows name subjects. By default, a caller's subject is `Principal.Subject` from your authenticator. Map callers to other subjects, or give staff roles at the workspace root, with `WithCallerAccess`:

```go
srv, err := server.New(store,
	server.WithEntities(&orgv1.OrgMember{}, &orgv1.Project{}, &orgv1.ProjectMember{}, &orgv1.Task{}),
	server.WithAuthenticator(authn),
	server.WithCallerAccess(func(_ context.Context, p server.Principal, workspaceID string) (server.CallerAccess, error) {
		access := server.CallerAccess{Subjects: []string{p.Issuer + "|" + p.Subject}}
		if staff, _ := p.StringClaim("staff"); staff == "admin" {
			access.RootRoles = []string{"org_admin"}
		}
		return access, nil
	}),
)
```

The hook replaces the default subject. Return `p.Subject` yourself if your grant rows name it. If you change the subjects, write grant rows with the same values. The hook runs on every request, so cache a lookup that is slow.

## 5. Create the first grant

A caller without a grant cannot enter the workspace, so write the first grant from trusted code, for example when you create the workspace:

```go
err := srv.Mutate(server.Trusted(ctx), workspaceID, func(tx *server.EntityTx) error {
	return tx.Put(&orgv1.OrgMember{Id: "m1", UserId: creatorID, Role: "org_admin"})
})
```

Until a grant exists, the caller's client reports `status().workspaces[id].access === 'none'`, keeps its queued writes, and syncs by itself once the grant exists. To create a workspace from the client, also offline, and to invite people, see [Create a workspace and invite people](create-a-workspace-and-invite-people.md).

## 6. Declare who may invoke each mutator

In a schema that uses the model, a mutator without `MutatorAccess` can only be invoked by trusted code. Name the permission and the row it is checked at:

```go
server.WithMutator("complete_task", &taskv1.CompleteTaskArgs{}, completeTask,
	server.MutatorAccess{Permission: "edit_tasks", Entity: "task", KeyField: "task_id"})
```

To let every caller that enters the workspace invoke a mutator, declare `server.MutatorAccess{Admitted: true}`.

`MutatorAccess` checks only that the caller holds the permission. It does not run the entity's rules, such as `when` and `within`, and the body's own writes are not checked. To apply the rules to a write, write it through `tx.AsCaller()`. The server then checks the write as the same write from the caller's client, and the write returns a `CodeAccessDenied` rejection when the rules refuse it:

```go
func completeTask(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
	id := args.(*taskv1.CompleteTaskArgs).GetTaskId()
	return tx.AsCaller().Patch(&taskv1.Task{Id: id, Done: true}, []string{"done"}, nil)
}
```

Read through `tx.AsCaller()` too when the body passes on what it reads, for example in a rejection message. Such a read returns what the caller's client would receive: a row the caller cannot read is not found, and a field that a guard hides from the caller has its zero value. A query whose predicate or order names such a field fails with `permission_denied`:

```go
func completeTask(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
	id := args.(*taskv1.CompleteTaskArgs).GetTaskId()
	var task taskv1.Task
	found, err := tx.AsCaller().Get(&task, id)
	if err != nil {
		return err
	}
	if !found || task.GetDone() {
		return server.Reject(server.CodeConflict, "the task is gone or already done")
	}
	return tx.AsCaller().Patch(&taskv1.Task{Id: id, Done: true}, []string{"done"}, nil)
}
```

Reads through `tx` itself see every row and every field.

Write through `tx` itself where the body must do what the caller cannot, such as giving the creator of a project its first admin grant. To ask about a permission that no rule checks, use `tx.Can("approve_tasks", &taskv1.Task{}, key)`. Add the permission to the `grants` of each role that gives it. Registration logs a warning for a permission that no rule uses. If no role grants the permission, `tx.Can` returns `server.ErrUnknownPermission`.

## 7. Shape the UI on the client

Use `can()` or the `useCan` hook to hide a control the caller cannot use. Ask about the operation the control does, not about a permission: the answer then follows every check of the rules, such as `when` and `within`. The answer is `'yes'`, `'no'` or `'unknown'`, and it is advisory: the server still decides each write.

```tsx
function EditButton({ taskId }: { taskId: string }) {
	const can = useCan(TaskSchema, taskId, 'update');
	return <button disabled={ can !== 'yes' }>Edit</button>;
}
```

- For a create, use `canCreate()` or `useCanCreate` with the row the control would create.
- Without `fields`, `can()` answers for some update, so a caller that may change only some fields gets `'yes'`. To decide each input of a form, pass the fields it changes: `useCan(CommentSchema, id, 'update', { fields: [ 'body' ] })`.
- For a permission that no rule checks, such as the one a mutator needs, use `hasPermission()` or `useHasPermission`.

See [Predicting access](../reference/typescript-client.md#predicting-access).

In tests that use `createTestClient`, give the caller roles with `server.setCallerAccess({ roles: [ { role: 'project_admin', at: [ ProjectSchema, 'p1' ] } ] })`. See [Testing your app without a server](test-your-app.md).
