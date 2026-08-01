# Sync status and failure codes

## `client.status()`

`client.status()` returns a snapshot and `client.onStatusChange(cb)` (or the
constructor option) subscribes — both work before `start()`:

```ts
interface SyncStatus {
	state: 'idle' | 'syncing' | 'error';
	online: boolean; // change stream connected or last sync succeeded
	dirty: boolean; // pendingMutations > 0, anywhere
	pendingMutations: number; // total across every workspace
	lastSyncedAt?: Date;
	// code is a SyncErrorCode; transportCode is the raw Connect code, if any.
	lastError?: { message: string; code?: string; transportCode?: string };
	// Per-workspace health; also reachable as client.ws(id).status().
	workspaces: Record<string, {
		workspaceId: string;
		pendingMutations: number;
		access?: 'granted' | 'revoked';
		scope?: { own: number; effective: number; widened: boolean };
	}>;
	// The default workspace's scope/access, mirrored so a single-workspace app
	// never has to index `workspaces`.
	scope?: { own: number; effective: number; widened: boolean };
	workspaceAccess?: 'granted' | 'revoked';
}
```

Connectivity, the sync-loop state and the schema-revision skew are
**account-wide** (one stream, one server build); pending counts, scope and access
are **per workspace**.

## Where failures show up

Failures reach the app on more than one channel, because they are not the same
kind of event: a mutation the server refused is not an outage, and neither is a
standing condition that retrying cannot clear.

| Failure                                                    | Channel                                             | Code                                                                       |
| ---------------------------------------------------------- | --------------------------------------------------- | -------------------------------------------------------------------------- |
| A mutation was refused by the app's rules                  | `onMutationRejected`                                | `permission_denied`, `invalid_argument`, `conflict`, or an app-defined one |
| The client gave up on a mutation the server kept failing   | `onMutationRejected`                                | `poison_dropped`                                                           |
| A purge discarded a queued mutation                        | `onMutationRejected` + the `removeWorkspace` result | `workspace_purged`                                                         |
| A local format upgrade could not salvage a queued mutation | `onMutationRejected` + `lastError` (a count)        | `format_upgraded`                                                          |
| A sync cycle failed                                        | `status().lastError`                                | the server's code, else `unavailable` / `internal`                         |
| A watch could not be registered                            | `status().lastError`                                | `watch_failed`                                                             |
| A text handle could not merge remote state                 | `TextHandle.status()` + `TextHandle.onError`        | `text_merge_failed`                                                        |
| Access to a workspace was withdrawn                        | `status().workspaces[id].access`                    | —                                                                          |
| Server and client schema revisions differ                  | `status().schemaSkew`                               | —                                                                          |

The rule of thumb: **`onMutationRejected` is per mutation** (something the user
wrote is gone, and the local state has already been rolled back before the
callback runs), **`lastError` is the sync loop** (transient, cleared by the next
successful cycle), and the last two are **standing conditions** that survive a
successful cycle because retrying cannot change them. The text-handle row is
neither: it belongs to one live handle rather than to the client, which is why
it has its own channel.

## Two codes that deliberately appear nowhere above

- **`text_full_state_required`** answers a collaborative-text PATCH whose
  delta-state fragment did not fit the row. The edit is intact — the coordinator
  re-sends it as a whole document — so surfacing it would report a loss that did
  not happen.
- **`text_state_stale`** answers one built on state a compaction dropped. The
  coordinator captures the field's visible text, re-bootstraps, and re-applies
  it, so the text survives (the marks the stale side minted over dropped
  characters do not). It reaches `onMutationRejected` only when that capture
  found nothing to restore.

## The code vocabulary

Rejection codes stay free-form on the wire so an app can invent its own, but
both sides ship constants for the common cases (`permission_denied`,
`invalid_argument`, `conflict`) and the client types its fields with a union
over them.

Codes are exported as constants (`CodeConflict`, `CodeWorkspacePurged`, …) with
`RejectionCode` and `SyncErrorCode` unions over them. Both fields widen the union
with `(string & {})`, so a code your own mutator invented still typechecks while
the known ones autocomplete — `switch` on it with a `default`.

`lastError.code` always speaks this vocabulary: a failure the server typed
carries its own code through, and anything else is classified `unavailable`
(unreachable, timed out, or a browser fetch that failed without saying why) or
`internal`. `transportCode` carries the underlying Connect code for the rare
caller that needs to tell, say, a `NotFound` from a `ResourceExhausted`.

## Callback isolation

A throw from your own callback is not one of these failures, and the library
does not let it become one. Every app callback it invokes — `onStatusChange`,
watch callbacks, `onMutationRejected`, `onUndoChange`, a text handle's
`subscribe`, `onSchemaSkew` — runs isolated: the commit or pull being reported
has already landed by the time it is called, so an exception is logged to
`console.error` and dropped rather than failing the write, skipping the
callbacks queued behind it, or marking the sync cycle errored. `onSchemaSkew` is
the one that owes an answer; a throwing policy falls back to `'warn'`, the action
an unconfigured one gets.

## Related

- [Handle failures](../how-to/handle-failures.md) — what to do about each.
