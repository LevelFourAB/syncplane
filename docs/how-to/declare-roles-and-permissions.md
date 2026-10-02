# Declare roles and permissions

This guide sets up the access model for an app where an organization has projects, projects have members, and tasks belong to projects. Org admins see and edit everything, and a project member sees only that project. For how the model works, see [The access model](../explanation/access-model.md).

## 1. Choose the permissions

Give each entity one permission to read its rows and one to write them. Use separate names for entities at different levels, so a role granted on the whole workspace does not reach more than you intend:

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
		access: { read: "view_members" write: "manage_members" }
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
				{ name: "project_admin" grants: [ "view_projects", "view_roster", "manage_roster", "view_tasks", "edit_tasks" ] }
			]
		}
		access: { read: "view_roster" write: "manage_roster" }
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string user_id = 3;
	string role = 4;
}
```

Role names are unique across the schema. A role granted to a caller can only be given or taken away by a caller who holds all of its permissions at the same row.

Every write refuses a role that the schema does not declare, but registration does not check rows that are already stored. When you add `grant` to an entity that already holds rows, or add a role, each stored row whose role value matches a declared role starts to grant it. Check the stored values before you deploy such a change.

## 3. Declare access on every other entity

Once one entity declares `access` or `grant`, every entity must declare `access`. Mark the reference that places a row below another row with `access_parent`:

```proto
message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		access: { read: "view_projects" write: "edit_projects" }
	};
	string id = 1;
	string name = 2;
}

message Task {
	option (syncplane.v1.entity) = {
		name: "task"
		key_fields: [ "id" ]
		access: { read: "view_tasks" write: "edit_tasks" }
	};
	string id = 1;
	Project project = 2 [(syncplane.v1.field_behavior) = { access_parent: {} }];
	string title = 3;
}
```

An entity that every member of the workspace may read and write declares `access: { unrestricted: true }`. Registration refuses a schema with a permission no role grants, or a role that grants a permission nothing uses. Read the error: it names the permission and the entity.

The TypeScript schema builder declares the same options with `access`, `grant` and `.accessParent()`; see the [TypeScript client reference](../reference/typescript-client.md#access-model).

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
