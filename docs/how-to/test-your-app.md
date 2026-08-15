# Test your app without a server

`syncplane/testing` runs your app's sync logic inside an ordinary test — any
runner (vitest, jest, `node:test`, bun) in a Node or browser environment: a
real `SyncClient` on an in-memory store, wired to an in-process fake server
that applies mutations with the engine's semantics. The behaviors that are
hard to observe in production — optimistic apply, rebase, rejection rollback,
offline queueing, re-bootstrap — become one-line arrangements.

```ts
import { createTestClient } from 'syncplane/testing';
import { TodoSchema } from './gen/todo_pb.js';

const t = await createTestClient({ entities: [ TodoSchema ] });
```

`t.client` is a real `SyncClient`; `t.db` is its `db` facade. Every
`SyncClientOptions` field can be passed through (`mutators`, `mergers`,
`subscriptions`, callbacks, …).

## The determinism model

Nothing moves unless the test says so. The kit disables polling, the subscribe
stream and presence, and defers the push debounce indefinitely — so writes
apply optimistically and queue, and the wire is touched only when you run a
cycle:

```ts
await t.db.todo.mutate.put({ id: 't1', title: 'Buy milk' });
expect(t.server.get(TodoSchema, 't1')).toBeUndefined(); // still queued
await t.sync(); // one push+pull cycle
expect(t.server.get(TodoSchema, 't1')).toBeDefined();
```

There are no timers to await and no sleeps to tune; assertions before and
after `sync()` see exactly the pre- and post-cycle state.

## Reject a write and assert the rollback

`server.reject` installs the rule an app's permission hook or mutator would
enforce. A rejection is an ack: the client drops the mutation, rolls the
optimistic edit back and fires `onMutationRejected` — the kit collects those
in `t.rejections`.

```ts
await t.net.offline();
await t.db.todo.mutate.put({ id: 't1', title: 'Not allowed' });

t.server.reject({ entity: TodoSchema, key: 't1' }, 'permission_denied');
await t.net.online(); // reconnects and runs a full cycle

expect(t.rejections[0]?.errorCode).toBe('permission_denied');
expect(await t.db.todo.get('t1')).toBeUndefined(); // what the user sees
```

Rules take a field match (`entity`, `key`, `op`, `mutator`, `clientId`,
`once`) or a predicate over the raw `Mutation`; the returned function removes
the rule.

While offline, every RPC fails as `unavailable` and writes keep queueing —
`status()` and rejection behavior are exactly the production offline path.

## Converge two clients

Share one server between clients to test what your app shows while replicas
catch up, and that it converges by per-field last-writer-wins:

```ts
import { createTestServer } from 'syncplane/testing';

const server = createTestServer({ entities: [ TodoSchema ] });
const a = await createTestClient({ server });
const b = await createTestClient({ server });

await a.db.todo.mutate.put({ id: 't1', title: 'Original' });
await a.sync();
await b.sync();

await a.db.todo.mutate.patch('t1', { title: 'Renamed' });
await b.db.todo.mutate.patch('t1', { completed: true });
await a.sync();
await b.sync();
await a.sync();
// both now hold { title: 'Renamed', completed: true }
```

## Seed and inspect server state

`server.put`/`server.del` write rows directly — seeding a workspace, or
playing the part of another client or a server-side effect. `server.get` and
`server.rows` read committed server state for assertions. Each write consumes
a sequence number, so synced clients receive it as an ordinary remote change
on their next cycle.

```ts
server.put(TodoSchema, { id: 'seeded', title: 'Already there' });
const t = await createTestClient({ server });
await t.sync();
```

Rows live in workspace `'main'` unless the call names another
(`{ workspace: 'other' }`).

## Force a re-bootstrap

`server.gc()` collects tombstones and advances the GC horizon to the current
sequence. A client that last synced before the call answers its next pull
with `reset` and transparently re-bootstraps — the path that otherwise only
shows up after a long-offline device comes back:

```ts
server.del(TodoSchema, 'stale');
server.gc();
await t.sync(); // re-bootstraps; 'stale' is gone locally, nothing thrown
```

## Hold pushes in flight

`server.holdPushes()` parks pushes at the server door — the state your UI is
in while a save is on the wire:

```ts
const hold = t.server.holdPushes();
const cycle = t.sync(); // don't await yet
// … assert spinners, status(), optimistic reads …
hold.release();
await cycle;
```

`t.server.pushes` records every `PushRequest` as it arrives, held or not.

## Custom mutators

Register the server half next to the client mirror and test both sides of the
contract — including that a divergence self-heals on rebase:

```ts
server.mutator('todo.clearCompleted', {
	apply(tx) {
		for (const row of tx.scan(TodoSchema)) {
			if (row.completed) tx.del(TodoSchema, row.id);
		}
	},
});
await t.client.mutate.custom('todo.clearCompleted');
await t.sync();
```

Throw `Rejection` from a mutator to reject the mutation; writes it staged are
discarded, like the engine's savepoint.

## What the kit does not simulate

Partial-sync predicates, windows and follows (every row syncs), server
queries (`queryServer` and friends fail), presence rooms, and multi-tab
behavior (Node has no shared lock manager). Cover those against the real
engine — see [Testing](testing.md) for the integration and e2e tiers.
