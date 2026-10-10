# Sync status and failure codes

This document describes the sync status properties, error channels, and failure codes in the TypeScript client.

## `client.status()`

`client.status()` returns a status snapshot. `client.onStatusChange(cb)` and the `onStatusChange` constructor option subscribe to status changes. Both work before `start()`.

Status properties have two scopes:

- **Account-wide**: Connectivity, sync-loop state, and schema-revision skew apply to the entire account across a single stream and server build.
- **Per-workspace**: Pending mutation counts, scope, access, and snapshot progress apply to individual workspaces. The client mirrors default workspace properties onto the top level of the status object.

For the full status object type definition, see the [client reference](typescript-client.md#sync-status).

## Where failures show up

The client reports failures and errors through specific channels based on their scope and lifecycle:

| Failure                                                      | Channel                                                                     | Code                                                                              |
| ------------------------------------------------------------ | --------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| A mutation was refused by application rules                  | `onMutationRejected`                                                        | `permission_denied`, `invalid_argument`, `conflict`, or a custom application code |
| The client dropped a mutation after repeated server failures | `onMutationRejected`                                                        | `poison_dropped`                                                                  |
| A purge operation discarded a queued mutation                | `onMutationRejected`, plus the `removeWorkspace` or `purgeLocalData` result | `workspace_purged`                                                                |
| A local format upgrade could not salvage a queued mutation   | `onMutationRejected`, plus a count in `lastError`                           | `format_upgraded`                                                                 |
| A sync cycle failed                                          | `status().lastError`                                                        | Server error code, `unavailable`, or `internal`                                   |
| A workspace queue was refused due to a mutation ID gap       | `status().lastError`, plus a `resequence` sync event                        | `mutation_id_gap`                                                                 |
| A watch could not be registered                              | `status().lastError`                                                        | `watch_failed`                                                                    |
| A text handle could not merge remote state                   | `TextHandle.status()`, `TextHandle.onError`                                 | `text_merge_failed`                                                               |
| The server withdrew a room                                   | `Room.status()`, `Room.onError`                                             | `room_dropped`                                                                    |
| A room publish message could not be sent                     | `Room.onError`                                                              | `room_offline`                                                                    |
| `Room.send` was called on a closed or dropped room           | The rejected `RoomSendError`                                                | `room_closed`, `room_dropped`                                                     |
| Access to a workspace was withdrawn                          | `status().workspaces[id].access` is `'revoked'`                             | None                                                                              |
| The caller holds no permission in a workspace yet            | `status().workspaces[id].access` is `'none'`                                | None                                                                              |
| Server and client schema revisions differ                    | `status().schemaSkew`                                                       | None                                                                              |

Failure reporting channels operate with different lifecycles:

- `onMutationRejected`: Reports per-mutation rejections. The client rolls back local mutation state before invoking the callback.
- `status().lastError`: Reports transient sync-loop errors. A successful sync cycle clears `lastError`.
- `status().workspaces[id].access` and `status().schemaSkew`: Report standing conditions that persist until the underlying condition changes.

Workspace access has these values:

- Unset: The server has not refused the workspace.
- `'revoked'`: Terminal. The application's `CanAccessWorkspace` denied the caller (`workspace_access_revoked` on the wire). The client stops syncing the workspace and keeps its local rows.
- `'none'`: Not terminal. The caller holds no permission in the workspace's access model (`workspace_no_access` on the wire). The client keeps the local rows and the queue, and asks again with backoff, 1 to 30 seconds apart, and at once on `syncNow()`.
- `'granted'`: The server admitted the caller after `'none'`. The client syncs the workspace again, queued writes included.
- `TextHandle.status()` and `TextHandle.onError`: Report errors on an individual live text handle without affecting client status.
- `Room.status()`, `Room.onError`, and `RoomSendError`: Report errors on an individual open room without affecting client status.

Room failure codes have the following behaviors:

- `room_dropped`: Terminal. The server re-evaluates access on every heartbeat.
- `room_offline`: Non-terminal. Indicates that a publish did not reach the server. The room re-sends state when the stream reconnects.
- Room data is not durable. Room failure codes do not indicate data loss. For more information, see [Rooms](../explanation/rooms.md).

## Two codes that deliberately appear nowhere above

The following codes are handled automatically and are not surfaced directly as client errors:

- **`text_full_state_required`**: Returned when a collaborative text `PATCH` delta-state fragment does not fit the row. The coordinator re-sends the update as a complete document.
- **`text_state_stale`**: Returned when a collaborative text `PATCH` relies on state removed by compaction. The coordinator captures the visible text in the field, re-bootstraps, and re-applies the edit. Visible text is preserved, but marks created over dropped characters are discarded. This code reaches `onMutationRejected` only when the capture finds no text to restore.

## The code vocabulary

Rejection codes are extensible strings. The client exports standard constants and union types:

- **Constants**: Named constants such as `CodeConflict` and `CodeWorkspacePurged`.
- **Types**: `RejectionCode` and `SyncErrorCode` union types. Both types widen the union with `(string & {})` to allow custom application codes while supporting autocomplete for standard codes. Handle these values using a `switch` statement with a `default` case.

The `lastError.code` property contains one of the following:

- A server-specified error code.
- `unavailable`: Indicates that the server is unreachable, a request timed out, or a browser fetch failed without an error reason.
- `internal`: Indicates an internal client error.

The `lastError.transportCode` property contains the underlying Connect transport code (for example, `NotFound` or `ResourceExhausted`).

## Callback isolation

Every application callback that the client invokes runs in isolation after the reported commit or pull completes. The client isolates the following callbacks:

- `onStatusChange`
- Watch callbacks
- `onMutationRejected`
- `onUndoChange`
- `TextHandle.subscribe`
- `onSchemaSkew`

An exception in a callback is logged to `console.error` and dropped. It does not fail the write, skip queued callbacks, or mark the sync cycle as errored.

If `onSchemaSkew` throws an error, the policy falls back to `'warn'`, which is the default behavior when no policy is configured.

## Related

- [Handle failures](../how-to/handle-failures.md): Guidance for handling client failures and errors.
