# Handling failures

This guide shows you how to handle mutation rejections, connection errors, revoked workspace access, and schema version skew in your application. Use this guide to notify users when operations fail and to recover from standing failure conditions.

For a complete listing of failure channels and error codes, see the [sync status and failure codes reference](../reference/sync-status-and-codes.md#where-failures-show-up).

## Prerequisites

Before you begin, ensure you have a configured `SyncClient` instance.

## Steps

1. **Handle rejected writes.** Configure the `onMutationRejected` callback to notify users when the server rejects a mutation. The client rolls back local state before the callback runs:

   ```ts
   const client = new SyncClient({
   	// ...
   	onMutationRejected: (info) => {
   		switch (info.errorCode) {
   			case CodeConflict:
   				return toast('Someone else changed this first');
   			case CodePermissionDenied:
   				return toast(info.errorMessage);
   			case CodeWorkspacePurged:
   				return; // handled by the code that called removeWorkspace
   			default:
   				return toast('Change could not be saved');
   		}
   	},
   });
   ```

   Codes are exported as constants with `RejectionCode` and `SyncErrorCode` unions over them. Both fields widen the union with `(string & {})`, so custom mutator codes typecheck while known codes autocomplete. Use a `switch` statement with a `default` case.

   Use `info.targets` to identify which rows were affected. For CRUD mutations, `info.targets` contains one entry. For batch mutations, it contains one entry per member. If you use a custom mutator, `info.targets` is empty and `info.mutator` contains the mutator name.

   If you use React, call `useOnMutationRejected(handler)` to tie the handler to the component lifecycle ([Build a UI with React](../reference/typescript-client.md#the-hooks)). Outside a constructor, call `client.onMutationRejected(cb)` to subscribe and receive an unsubscribe function.

   If a push response is lost, the server records the verdict with the watermark and replays it on the resend. The client keeps the mutation queued until the resend collects it ([the convergence model](../explanation/convergence-model.md#rejections-are-results-not-errors)). Because the rejection arrives one sync cycle late, ensure that handlers closing dialogs or dropping drafts can safely run after a delay.

2. **Monitor connection status.** Register an `onStatusChange` listener to update your UI when connectivity changes or when mutations are pending:

   ```ts
   client.onStatusChange((s) => {
   	setBanner(s.state === 'error' ? s.lastError?.message : undefined);
   	setPending(s.pendingMutations);
   });
   ```

   `start()` returns immediately and does not throw on the first sync. An unreachable server surfaces as `state: "error"` while mutations stay queued, and sync recovers automatically with backoff.

   If local setup fails before sync begins (such as a missing `url` or `transport`, or a local store that fails to open), `start()` rejects. Fix the cause or wait for the store to recover, then call `start()` again. Concurrent callers share one start attempt.

3. **Handle revoked workspace access.** Check the access state and remove revoked workspaces from the client.

   When the server permanently denies access to a workspace (using a `WorkspaceDropped` frame with reason `ACCESS_REVOKED` or a `workspace_access_revoked` error on a push batch), the client stops syncing that workspace and sets `status().workspaces[id].access` to `'revoked'`. Other workspaces on the same stream continue syncing.

   The client does not automatically delete the local copy. To remove the workspace and handle discarded rows, run:

   ```ts
   if (client.status().workspaces[id]?.access === 'revoked') {
   	const { discarded } = await client.removeWorkspace(id, { purge: true });
   	if (discarded.length) await stashSomewhere(discarded);
   }
   ```

4. **Handle schema version skew during deployment.** Configure `schemaRevision` and `onSchemaSkew` to detect and respond to schema differences between the client and server.

   When the server uses `WithSchemaRevision`, it advertises an application-defined revision on every stream frame, push, pull, server query, and bootstrap. Configure `schemaRevision` on the client to compare revisions. When a mismatch occurs, the client runs `onSchemaSkew`. Use this callback to surface an update notification, switch to read-only mode (`block`), or force a clean re-bootstrap (`resync`).

   This makes the skew window during a rolling deployment visible and actionable instead of silently dropping writes.

## Confirming the result

To verify your failure handling setup:

- Trigger a write conflict and confirm that `onMutationRejected` runs and displays the expected notification.
- Disconnect the network and verify that `onStatusChange` reports `state: "error"` with `lastError` and updates the pending mutation count.
- Revoke workspace access on the server and confirm that `status().workspaces[id]?.access` updates to `'revoked'`.

## Related

- [Callback isolation](../reference/sync-status-and-codes.md#callback-isolation)
- [Sync status and failure codes](../reference/sync-status-and-codes.md)
- [Authentication and permissions](authentication-and-permissions.md)
