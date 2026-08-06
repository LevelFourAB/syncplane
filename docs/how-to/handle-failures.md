# Handle failures

Failures reach the app on more than one channel, because they are not the same
kind of event: a mutation the server refused is not an outage, and neither is a
standing condition that retrying cannot clear. The rule of thumb:

- **`onMutationRejected` is per mutation.** Something the user wrote is gone, and
  the local state has already been rolled back before the callback runs.
- **`status().lastError` is the sync loop.** Transient, cleared by the next
  successful cycle.
- **Access revocation and schema skew are standing conditions.** They survive a
  successful cycle, because retrying cannot change them.

The [full table of failures, channels and
codes](../reference/sync-status-and-codes.md#where-failures-show-up) is in the
reference.

## Tell the user a write was refused

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

Codes are exported as constants with `RejectionCode` and `SyncErrorCode` unions
over them. Both fields widen the union with `(string & {})`, so a code your own
mutator invented still typechecks while the known ones autocomplete — `switch`
on it with a `default`.

`info.targets` says WHAT died: the rows the mutation would have written, one
entry for a CRUD mutation and one per member for a batch, so a message can name
the thing the user lost rather than "a change". A custom mutator writes no rows
by this route, so its `targets` is empty and `info.mutator` carries its name.

In React, `useOnMutationRejected(handler)` is the same surface with a component's
lifetime ([Build a UI with React](react.md#the-hooks)); outside a constructor,
`client.onMutationRejected(cb)` subscribes and returns an unsubscribe, so several
listeners can coexist with the option above.

> **This callback is best-effort, not durable.** A rejection only bumps the
> idempotency watermark, so a lost push response degrades to a silent drop: the
> retry comes back `ALREADY_APPLIED` and the edit vanishes at the next rebase.
> An app that needs guaranteed feedback must persist results keyed by
> client/mutation id until acknowledged.

## Show connectivity

```ts
client.onStatusChange((s) => {
	setBanner(s.state === 'error' ? s.lastError?.message : undefined);
	setPending(s.pendingMutations);
});
```

`start()` returns immediately and never throws on the first sync: an unreachable
server surfaces as `state: "error"` while mutations stay queued, and sync
recovers automatically with backoff.

What it _can_ reject on is the local set-up it awaits before sync begins —
missing `url`/`transport`, or a local store that will not open. Such a start
leaves the client startable: fix the cause (or wait out a flaky store) and
call `start()` again. Concurrent callers share one start attempt.

## Handle losing access to a workspace

When the server permanently denies a workspace — a `WorkspaceDropped` frame with
reason `ACCESS_REVOKED`, or `workspace_access_revoked` on a push batch — the
client stops syncing that workspace and reports it on
`status().workspaces[id].access`. Other workspaces on the same stream are
unaffected.

The local copy is **not** deleted automatically: discarding a user's rows on one
server verdict is an app decision.

```ts
if (client.status().workspaces[id]?.access === 'revoked') {
	const { discarded } = await client.removeWorkspace(id, { purge: true });
	if (discarded.length) await stashSomewhere(discarded);
}
```

## Handle a version skew during a deploy

`WithSchemaRevision` makes the server advertise an app-defined revision on every
stream frame, push, pull, server query and bootstrap. A client configured with
its own `schemaRevision` compares the two and runs `onSchemaSkew` on a
mismatch — surface "update required" on the status, go read-only (`block`), or
force a clean re-bootstrap (`resync`).

This does not replace additive schema evolution; it makes a skew window during a
rolling deploy visible and actionable instead of a silent stream of dropped
writes.

## A throw from your own callback is contained

Every app callback the library invokes — `onStatusChange`, watch callbacks,
`onMutationRejected`, `onUndoChange`, a text handle's `subscribe`,
`onSchemaSkew` — runs isolated. The commit or pull being reported has already
landed by the time it is called, so an exception is logged to `console.error`
and dropped rather than failing the write, skipping the callbacks queued behind
it, or marking the sync cycle errored.

`onSchemaSkew` is the one that owes an answer; a throwing policy falls back to
`'warn'`, the action an unconfigured one gets.

## Related

- [Sync status and failure codes](../reference/sync-status-and-codes.md).
- [Authentication and permissions](authentication-and-permissions.md) — the
  server side of a rejection.
