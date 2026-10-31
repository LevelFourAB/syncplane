# Deleting a workspace

This guide shows you how to delete a workspace on the server, and how clients that still sync it stop and clear their local copy. Use it when the thing a workspace holds goes away: a project is closed, or a local folder that backs a graph is removed.

## Prerequisites

Before you begin, verify that you have:

- A `*server.Server` in your backend.
- A place in your backend that decides who may delete a workspace. `DeleteWorkspace` runs no permission hook.

## Steps

1. Delete the workspace on the server:

   ```go
   if err := srv.DeleteWorkspace(ctx, workspaceID); err != nil {
   	return err
   }
   ```

   The call removes every row, tombstone, client watermark, text delta, and consumer position of the workspace in one storage transaction. From then on the server refuses the workspace ID: sync clients receive `workspace_access_revoked`, and in-process calls return an error that matches `server.ErrWorkspaceDeleted`.

   Do not call it from inside a mutator or another write transaction.

2. Clear the local copy on each client:

   A client that syncs the workspace stops syncing it and sets `status().workspaces[id].access` to `'revoked'`. It keeps its local rows and its queue. Remove them when the status changes:

   ```ts
   const client = new SyncClient({
   	// ...
   	onStatusChange: (status) => {
   		for (const [ id, ws ] of Object.entries(status.workspaces)) {
   			if (ws.access === 'revoked') {
   				void client.removeWorkspace(id, { purge: true });
   			}
   		}
   	},
   });
   ```

   `removeWorkspace` returns the mutations that were still queued in `discarded`. The server refused them, so this is their only copy.

3. Reuse the ID, if the workspace comes back:

   The server refuses a deleted ID until you lift the refusal:

   ```go
   if err := srv.RestoreWorkspace(ctx, workspaceID); err != nil {
   	return err
   }
   ```

   `RestoreWorkspace` brings back no data. The next write starts a new, empty workspace under the ID. A client that removed the workspace adds it again with `addWorkspace`.

## Confirming the result

To confirm that the workspace is deleted:

1. Call `srv.WorkspaceIDs(ctx)` and verify that the result does not contain the workspace ID.
2. Run `client.syncNow()` on a client that synced the workspace, and verify that `client.status().workspaces[id]?.access` is `'revoked'`.

## Related

- [Go server API: Deleting a workspace](../reference/server.md#deleting-a-workspace)
- [Handle failures](handle-failures.md)
- [Hold several workspaces on one client](multiple-workspaces.md)
