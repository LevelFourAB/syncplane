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

A grant entity holds one row per subject and role. Its rows apply at the row its `access_parent` field points at, or at the workspace root when it has no such field.

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
				{ read: true permission: "view_members" },
				{ write: true permission: "manage_members" }
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
			allow: [
				{ read: true permission: "view_roster" },
				{ write: true permission: "manage_roster" }
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string user_id = 3;
	string role = 4;
}
```

`project_admin` includes `project_reader`, so it grants every permission of `project_reader` and the two it lists. When you add a permission to `project_reader`, `project_admin` grants it too. A role can include only roles of its own `grant`, and a role can include a role that includes other roles. Registration refuses a role that includes itself through any chain.

Role names are unique across the schema. A role granted to a caller can only be given or taken away by a caller who holds all of its permissions at the same row.

Every write refuses a role that the schema does not declare, but registration does not check rows that are already stored. When you add `grant` to an entity that already holds rows, or add a role, each stored row whose role value matches a declared role starts to grant it. Check the stored values before you deploy such a change.

### Give a role to a team

First choose how your app uses teams:

| Your app needs                                                     | Use                                                                                     |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| Each project belongs to one team                                   | Make `Team` the `access_parent` of `Project`, and grant roles on the team.              |
| Teams come from your identity provider                             | Return the caller's groups from `WithSubjects`, such as `group:eng`, and grant to them. |
| A team gets a role on projects that other teams or users also have | Team grants, below.                                                                     |

For team grants, declare who the members of a team are with `members` on the team entity. Each `team_member` row makes its user a member of its team, whatever the role:

```proto
message Team {
	option (syncplane.v1.entity) = {
		name: "team"
		key_fields: [ "id" ]
		members: { entity: "team_member" }
		access: {
			allow: [
				{ read: true permission: "view_teams" },
				{ write: true permission: "manage_teams" }
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
			allow: [
				{ read: true permission: "view_teams" },
				{ write: true permission: "manage_teams" }
			]
		}
	};
	string id = 1;
	Team team = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string user_id = 3;
	string role = 4;
}
```

Then let `ProjectMember` name a user or a team. Make its subject a oneof, and keep the roles you have:

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
				{ name: "project_admin" grants: [ "view_projects", "view_roster", "manage_roster", "view_tasks", "edit_tasks" ] }
			]
		}
		access: {
			allow: [
				{ read: true permission: "view_roster" },
				{ write: true permission: "manage_roster" }
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

A `ProjectMember` row with `team` set gives its role to every member of that team. When someone joins or leaves the team, their clients receive or lose the project's rows on the next pull. One roster holds users and teams, so the UI lists both from one entity.

Before you ship, check these points:

- **Who manages a team decides who gets its grants.** A grant to a team gives its role to every user added to the team later. Give `manage_teams` only to callers you trust with every grant the team holds.
- **Every role of the member entity makes a member.** For guests who see a team but do not get its grants, use a separate grant entity anchored at the team.
- **A team cannot be a member of a team.** Registration refuses a member entity whose subject is a team.
- **To give teams other roles than users**, declare a separate grant entity with a plain `Team team` subject and its own roles. Role names are unique across the schema, so name them differently, such as `project_team_reader`.
- **To let a user leave a project**, add `{ delete: true permission: "view_roster" subject: "user_id" }`. A team's row holds no `user_id`, so the rule does not let a member remove the team.
- **Delete the grants before the team.** A client cannot delete a team that a live grant row names.
- **Admins are members only with a row.** A role at the workspace root does not make a caller a member of a team. To let an admin see what a team sees, grant the admin the same permissions at the root.

## 3. Declare access on the entities the roles govern

An entity that declares no `access` stays open to every caller that enters the workspace, so declare `access` on each entity whose rows a role decides, and leave the others alone. Mark the reference that places a row below another row with `access_parent`; the entity it points at must declare `access` too:

```proto
message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true permission: "view_projects" },
				{ write: true permission: "edit_projects" }
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
			allow: [
				{ read: true permission: "view_tasks" },
				{ write: true permission: "edit_tasks" }
			]
		}
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
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
			allow: [
				{ read: true permission: "view_tasks" },
				{ write: true permission: "edit_tasks" }
			]
		}
	};
	string id = 1;
	Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
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
			allow: [
				{ read: true permission: "view_folders" },
				{ write: true permission: "edit_folders" }
			]
		}
	};
	string id = 1;
	Folder parent = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
}
```

A chain is at most `Limits.MaxAccessDepth` rows long (32 by default). The server refuses a folder moved below itself or below a chain that is already that long.

A row can sit below one of several entities, such as a comment on a task or on a doc. Mark every member of a oneof with `access_parent`. The case a row holds decides where it sits:

```proto
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true permission: "view_comments" },
				{ write: true permission: "edit_comments" }
			]
		}
	};
	string id = 1;
	oneof subject {
		Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
		Doc doc = 3 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	}
	string body = 4;
}
```

The comment's rules apply in both cases. Add `view_comments` and `edit_comments` to the project roles and to the folder roles, so that a role in either tree reaches the comments below it. A reference message whose target is a oneof of entities works the same way: mark the field that holds it. See [Polymorphic access parents](../reference/proto-options.md#polymorphic-access-parents).

Registration refuses a schema with a permission no role grants, or a role that grants a permission nothing uses. Read the error: it names the permission and the entity.

The TypeScript schema builder declares the same options with `access`, `grant` and `.accessParent()`; see the [TypeScript client reference](../reference/typescript-client.md#access-model).

### Allow some operations and not others

Each rule names the operations it admits. An operation is allowed when at least one rule admits it. An operation that no rule names is open only to trusted writes: code that runs under `server.Trusted`, connectors, effects and the bodies of custom mutators. The server refuses a client write that can only be such an operation before `CanMutate` runs, and the TypeScript client rejects it at the call site.

To give create, update and delete to different permissions, write one rule for each. Here a member who may comment creates comments, and a moderator changes and deletes them:

```proto
message Comment {
	option (syncplane.v1.entity) = {
		name: "comment"
		key_fields: [ "id" ]
		access: {
			allow: [
				{ read: true permission: "view_tasks" },
				{ create: true permission: "post_comments" },
				{ update: true delete: true permission: "moderate_comments" }
			]
		}
	};
	string id = 1;
	Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
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
		access: { allow: [ { read: true permission: "view_tasks" } ] }
	};
	string id = 1;
	Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
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
			allow: [
				{ read: true permission: "view_tasks" },
				{ create: true permission: "edit_tasks" }
			]
		}
	};
	string id = 1;
	Task task = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
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
				{ write: true permission: "edit_projects" }
			]
		}
	};
	string id = 1;
	string name = 2;
}
```

An entity whose rules use `admitted` stays in the model. It can have an access parent, be the access parent of other entities, and be a grant entity.

To keep a client from deleting rows in a schema that does not use the access model, refuse the delete in `CanMutate`. See [Reserve an operation for trusted writes](authentication-and-permissions.md#reserve-an-operation-for-trusted-writes).

### Moving and deleting rows that other rows sit below

Rows below a row move with it. To move a task to another project, the caller needs one update rule that admits it at both projects, here the `write` rule with `edit_tasks`, and, at the old project, the read permission of every entity that can sit below a task, here `view_tasks` for the comments. Without the read permission a caller could carry rows it cannot read to a project where other people can read them.

A client cannot delete a row while a live row names it as its access parent. Delete or move the comments first; one batch can delete the comments and the task together. Trusted code can delete the task anyway, and its comments then sit below nothing, so only a caller who holds `view_tasks` at the workspace root reads them.

## 4. Tell the server who the caller is

Grant rows name subjects. By default, a caller's subject is `Principal.Subject` from your authenticator. Map callers to more subjects, or give staff roles on every workspace, with two server options:

```go
srv, err := server.New(store,
	server.WithEntities(&orgv1.OrgMember{}, &orgv1.Project{}, &orgv1.ProjectMember{}, &orgv1.Task{}),
	server.WithAuthenticator(authn),
	server.WithSubjects(func(p server.Principal) []string {
		return []string{p.Issuer + "|" + p.Subject}
	}),
	server.WithGlobalRoles(func(_ context.Context, p server.Principal) ([]string, error) {
		if staff, _ := p.StringClaim("staff"); staff == "admin" {
			return []string{"org_admin"}, nil
		}
		return nil, nil
	}),
)
```

If you change the subjects, write grant rows with the same values.

## 5. Create the first grant

A caller without a grant cannot enter the workspace, so write the first grant from trusted code, for example when you create the workspace:

```go
err := srv.Mutate(server.Trusted(ctx), workspaceID, func(tx *server.EntityTx) error {
	return tx.Put(&orgv1.OrgMember{Id: "m1", UserId: creatorID, Role: "org_admin"})
})
```

## 6. Declare who may invoke each mutator

In a schema that uses the model, a mutator without `MutatorAccess` can only be invoked by trusted code. Name the permission and the row it is checked at:

```go
server.WithMutator("complete_task", &taskv1.CompleteTaskArgs{}, completeTask,
	server.MutatorAccess{Permission: "edit_tasks", Entity: "task", KeyField: "task_id"})
```

The body's own writes are not checked. Use `tx.Can("edit_tasks", &taskv1.Task{}, key)` in the body where a write needs the caller's permission.

## 7. Shape the UI on the client

Use `can()` or the `useCan` hook to hide a control the caller cannot use. The answer is `'yes'`, `'no'` or `'unknown'`, and it is advisory: the server still decides each write.

```tsx
function EditButton({ taskId }: { taskId: string }) {
	const can = useCan(TaskSchema, taskId, 'edit_tasks');
	return <button disabled={ can !== 'yes' }>Edit</button>;
}
```

In tests that use `createTestClient`, set the summary the client receives with `server.setCallerAccess(...)`.
