# Create a workspace and invite people

This guide gives the creator of a workspace their first role, lets a client create a workspace while it is offline, and invites other people. It continues [Declare roles and permissions](declare-roles-and-permissions.md) and uses its `OrgMember` and `ProjectMember` grant entities.

## How access starts

In a schema that declares a role, the server refuses a caller that holds no permission in the workspace, before it runs any write or mutator. Two things follow:

- **Trusted code writes the first grant.** No client can write it, because no client holds a permission yet. Write it from your backend.
- **A client waits for a grant without losing work.** Until a grant exists, the client reports `status().workspaces[id].access === 'none'`, keeps the rows and the queued writes, and asks again with backoff. When a grant exists, it syncs the workspace, queued writes included, and reports `'granted'`.

## 1. Write the first grant from your backend

Add an endpoint that creates the workspace for the signed-in user. It writes the creator's grant with `server.Trusted`. Refuse a workspace id that already holds grant rows, or any caller could make themselves admin of a workspace that exists:

```go
var errWorkspaceTaken = errors.New("workspace id is taken")

func createWorkspace(srv *server.Server, workspaceID, userID string) error {
	return srv.Mutate(server.Trusted(context.Background()), workspaceID, func(tx *server.EntityTx) error {
		taken := false
		if _, _, err := tx.ScanPage(&orgv1.OrgMember{}, "", 1, func(proto.Message) error {
			taken = true
			return nil
		}); err != nil {
			return err
		}
		if taken {
			return errWorkspaceTaken
		}
		return tx.Put(&orgv1.OrgMember{Id: "om-" + userID, UserId: userID, Role: "org_admin"})
	})
}
```

`Mutate` holds the workspace's write lock, so two requests for the same id cannot both find it free. Use the same `userID` that `WithCallerAccess` returns first in `Subjects` for the caller, or `Principal.Subject` when you do not set `WithCallerAccess`.

## 2. Create the workspace from the client

The client chooses the id, so it can create the workspace and write to it while it is offline:

```ts
const workspaceId = crypto.randomUUID();
const ws = client.addWorkspace(workspaceId);
await ws.db.project.mutate.put({
	id: crypto.randomUUID(),
	name: 'First project',
});
```

The writes wait in the queue, and `access` is `'none'` until the endpoint has run. When the device is online, call the endpoint, then sync at once:

```ts
await fetch('/api/workspaces', {
	method: 'POST',
	body: JSON.stringify({ workspaceId }),
});
await client.syncNow();
```

Without the `syncNow()` call, the client finds the grant on its next retry, at most 30 seconds later.

If the endpoint refuses the id, create a new id and move the rows to it. The queued writes for the refused id stay in the queue until you call `client.removeWorkspace(workspaceId, { purge: true })`, which returns them.

## 3. Add a person who has an account

A caller who holds `manage_members` writes a grant row for the other person's user id from the client. The access rules check the write, and the caller can give only a role whose permissions it holds itself:

```ts
await client.db.orgMember.mutate.put({
	id: `om-${userId}`,
	userId,
	role: 'org_member',
});
```

The new member's client adds the workspace with `client.addWorkspace(workspaceId)`. If it adds the workspace before the grant reaches the server, it reports `'none'` and syncs by itself when the grant exists.

## 4. Invite a person by email

A person who has no account yet has no user id to grant to. Grant to their email address instead, as a subject:

1. Map each caller to an email subject in `WithCallerAccess`, from a verified email claim only. Keep the user id first, because a `creator` field stores the first subject:

   ```go
   server.WithCallerAccess(func(_ context.Context, p server.Principal, _ string) (server.CallerAccess, error) {
   	subjects := []string{p.Subject}
   	verified, _ := p.Claim("email_verified")
   	if email, ok := p.StringClaim("email"); ok && verified == true {
   		subjects = append(subjects, "email:"+strings.ToLower(email))
   	}
   	return server.CallerAccess{Subjects: subjects}, nil
   })
   ```

2. Let the inviter write a grant row whose subject is the email subject. The access rules check it as in step 3:

   ```ts
   const subject = `email:${email.toLowerCase()}`;
   await client.db.projectMember.mutate.put({
   	id: `pm-${projectId}-${subject}`,
   	project: { id: projectId },
   	userId: subject,
   	role: 'project_reader',
   });
   ```

3. Send the person a link that holds the workspace id. When they sign in with that email, they hold the role at once. Their client adds the workspace from the link.

To remove an invitation, delete its grant row. To replace an email grant with a user-id grant after sign-in, write the new row and delete the old one in one batch.

## Check before you ship

- **Refuse a workspace id that holds grant rows.** The create endpoint runs as trusted code, so the access rules do not stop it.
- **Use verified email addresses only.** An unverified email claim lets a caller take the grants of any address. Write the same form of the address in `WithCallerAccess` and in the grant rows, such as lowercase.
- **Show the waiting state.** Show the user that they wait for access while `access` is `'none'`. If they decline an invitation, call `client.removeWorkspace(id)` to stop the retries.
- **A caller who loses their last grant also sees `'none'`.** Their client keeps the rows it holds. Remove the workspace with `client.removeWorkspace(id, { purge: true })` when your app decides the rows must not stay on the device.
