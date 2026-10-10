# Share a project with the whole workspace or with chosen teams

This guide makes each project either **shared** or **restricted**:

- Every member of the workspace reads and edits a shared project and the tasks in it.
- Only the users and teams with a grant on a restricted project read it, together with the workspace admins.

A project admin changes a project from one to the other with one update. Clients receive or lose the project and its tasks on their next pull.

This guide continues [Declare roles and permissions](declare-roles-and-permissions.md). Read that guide first, and its section [Give a role to a team](declare-roles-and-permissions.md#give-a-role-to-a-team).

## How it works

A role applies at the row its grant anchors at, and at every row below that row. This guide uses one **commons** row:

- The role that every member of the workspace holds anchors at the commons row, not at the workspace root.
- A shared project sits below the commons row, so every member reaches it.
- A restricted project sits at the root. Only roles at the root, and grants on the project itself, reach it.

To share or restrict a project, you change one reference field. The engine sees this change as a move, so it sends the correct rows to each client. You do not need a grant row for each member on each project.

## 1. Declare the commons and the workspace roles

Declare an entity for the commons. It has one row. Every admitted caller can read it, and only trusted writes create it:

```proto
message Commons {
	option (syncplane.v1.entity) = {
		name: "commons"
		key_fields: [ "id" ]
		access: { allow: [ { read: true admitted: {} } ] }
	};
	string id = 1;
}
```

Anchor the member role at the commons row. Give it the permissions that every member holds on a shared project:

```proto
message WorkspaceMember {
	option (syncplane.v1.entity) = {
		name: "workspace_member"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [ {
				name: "workspace_member"
				grants: [ "view_members", "view_projects", "edit_projects", "view_roster", "view_tasks", "edit_tasks" ]
			} ]
		}
		access: {
			allow: [
				{ read: true requires: "view_members" },
				{ write: true requires: "manage_members" }
			]
		}
	};
	string id = 1;
	Commons commons = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string user_id = 3;
	string role = 4;
}
```

Workspace admins need their role at the root, so that it reaches restricted projects too. Declare a grant entity without an `access_parent` field for them, or return the role in `RootRoles` from `WithCallerAccess`:

```proto
message Admin {
	option (syncplane.v1.entity) = {
		name: "admin"
		key_fields: [ "id" ]
		grant: {
			subject: "user_id"
			role: "role"
			roles: [ {
				name: "admin"
				grants: [
					"view_admins", "manage_admins",
					"view_members", "manage_members",
					"view_projects", "edit_projects",
					"view_roster", "manage_roster",
					"view_tasks", "edit_tasks"
				]
			} ]
		}
		access: {
			allow: [
				{ read: true requires: "view_admins" },
				{ write: true requires: "manage_admins" }
			]
		}
	};
	string id = 1;
	string user_id = 2;
	string role = 3;
}
```

Do not use one grant entity for both roles. A `WorkspaceMember` row with an empty `commons` field grants nothing.

## 2. Give the project an access parent at the commons

Add a `commons` reference to the project, and mark it as the access parent. A project with `commons` set is shared. A project with `commons` empty is restricted:

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
	Commons commons = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string name = 3;
}
```

Keep `Task` and `ProjectMember` as they are, with their `access_parent` on the project. A role on the commons row reaches them through a shared project.

## 3. Give roles on restricted projects to users and teams

Use the `ProjectMember` entity from [Give a role to a team](declare-roles-and-permissions.md#give-a-role-to-a-team): its subject is a oneof of a user and a team. Give `project_admin` the permissions of the workspace member role and `manage_roster`. A grant on a project stays when the project changes between shared and restricted.

## 4. Create the commons row

Write the commons row and the first admin grant from trusted code when you create the workspace. Give each new member a `WorkspaceMember` row anchored at the commons row:

```go
err := srv.Mutate(server.Trusted(ctx), workspaceID, func(tx *server.EntityTx) error {
	if err := tx.Put(&orgv1.Commons{Id: "main"}); err != nil {
		return err
	}
	if err := tx.Put(&orgv1.Admin{Id: "a1", UserId: creatorID, Role: "admin"}); err != nil {
		return err
	}
	return tx.Put(&orgv1.WorkspaceMember{Id: "m1", Commons: &orgv1.Commons{Id: "main"}, UserId: creatorID, Role: "workspace_member"})
})
```

## 5. Share and restrict a project

Set the `commons` field to share a project, and clear it to restrict the project:

```ts
// Share with every member of the workspace.
await client.db.project.mutate.patch(projectId, { commons: { id: 'main' } });

// Restrict to the project's own grants. The mask names the field to clear.
await client.db.project.mutate.patch(projectId, {}, { paths: [ 'commons' ] });
```

The server accepts the change only from a caller that holds `edit_projects` where the project is before the change and where it is after the change. The caller must also hold `view_tasks` and `view_roster` where the project is before the change, because the tasks and grants move with it:

| Change                      | Who can do it                             |
| --------------------------- | ----------------------------------------- |
| Share a restricted project  | A project admin, or an admin at the root. |
| Restrict a shared project   | A project admin, or an admin at the root. |
| Create a shared project     | Every member of the workspace.            |
| Create a restricted project | An admin at the root.                     |
| Rename a shared project     | Every member of the workspace.            |

A member who holds `edit_projects` only through the commons row cannot restrict a shared project. After the change, the project is not below the commons row, so the member does not hold `edit_projects` there.

To let every member create a restricted project, write a custom mutator. Its body creates the project and a `project_admin` grant for the caller in one transaction. See [Declare who may invoke each mutator](declare-roles-and-permissions.md#6-declare-who-may-invoke-each-mutator).

`can(ProjectSchema, id, 'update')` answers `'yes'` on a shared project to every member, because the commons row gives them `edit_projects`, so do not use it to show the share control. Ask with `useHasPermission` for a permission that only project admins and root admins hold, such as `manage_roster`:

```tsx
function ShareToggle({ projectId }: { projectId: string }) {
	const can = useHasPermission(ProjectSchema, projectId, 'manage_roster');
	return <ShareSwitch disabled={ can !== 'yes' } />;
}
```

## 6. Check before you ship

- **Do not grant the member role at the root.** A role at the root reaches restricted projects too.
- **Place rows outside projects on purpose.** Members hold no permission at the root. Use `admitted` rules for rows that every member reads, such as labels, or put the rows below the commons row.
- **A restricted project's key is not secret.** A member who loses a project receives a tombstone with its key. See [Costs and limits](../explanation/access-model.md#costs-and-limits).
- **Grants on a shared project stay.** When a project becomes restricted, a member with a grant of their own on the project keeps it.
