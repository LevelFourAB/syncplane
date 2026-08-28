# Testing your app without a server

This guide shows you how to test your application's sync logic without running a remote server. Use `syncplane/testing` to run a real `SyncClient` on an in-memory store connected to an in-process test server.

## Prerequisites

Before you begin, ensure you have:

- A test runner such as Vitest, Jest, `node:test`, or Bun in a Node.js or browser environment.
- Generated schema definitions for your entities (for example, `TodoSchema`).

1. Initialize a test client:

   Import `createTestClient` from `syncplane/testing` and pass your entity schemas:

   ```ts
   import { createTestClient } from 'syncplane/testing';
   import { TodoSchema } from './gen/todo_pb.js';

   const t = await createTestClient({ entities: [ TodoSchema ] });
   ```

   `t.client` is a `SyncClient` instance, and `t.db` is its database facade. You can pass any `SyncClientOptions` fields into `createTestClient`, including `mutators`, `mergers`, `subscriptions`, and callbacks.

2. Trigger sync cycles manually:

   Apply local mutations and call `t.sync()` to run a push and pull cycle. The testing kit disables polling, presence, and the subscribe stream, and defers push debouncing indefinitely:

   ```ts
   await t.db.todo.mutate.put({ id: 't1', title: 'Buy milk' });
   expect(t.server.get(TodoSchema, 't1')).toBeUndefined(); // still queued
   await t.sync(); // one push+pull cycle
   expect(t.server.get(TodoSchema, 't1')).toBeDefined();
   ```

   Assertions before and after `t.sync()` inspect the pre-cycle and post-cycle states directly without timers or sleeps.

3. Reject a write and assert the rollback:

   Use `t.server.reject` to install rejection rules for permission hooks or mutators. When rejected, the client drops the mutation, rolls back the optimistic edit, and triggers `onMutationRejected`:

   ```ts
   await t.net.offline();
   await t.db.todo.mutate.put({ id: 't1', title: 'Not allowed' });

   t.server.reject({ entity: TodoSchema, key: 't1' }, 'permission_denied');
   await t.net.online(); // reconnects and runs a full cycle

   expect(t.rejections[0]?.errorCode).toBe('permission_denied');
   expect(await t.db.todo.get('t1')).toBeUndefined(); // what the user sees
   ```

   Rules accept field matches (`entity`, `key`, `op`, `mutator`, `clientId`, `once`) or a predicate over the raw `Mutation`. The returned function removes the rule. While offline, every remote procedure call (RPC) fails as `unavailable` and writes continue queueing.

4. Test convergence across multiple clients:

   Share one server instance between clients to verify that replicas converge using per-field last-writer-wins:

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

5. Seed and inspect server state:

   Use `server.put` and `server.del` to write rows directly to the server, and use `server.get` and `server.rows` to inspect committed server state:

   ```ts
   server.put(TodoSchema, { id: 'seeded', title: 'Already there' });
   const t = await createTestClient({ server });
   await t.sync();
   ```

   Each write consumes a sequence number, so synced clients receive it as a remote change on their next sync cycle. Rows reside in workspace `'main'` unless you specify another workspace with `{ workspace: 'other' }`.

6. Force a client re-bootstrap:

   Call `server.gc()` to collect tombstones and advance the garbage collection (GC) horizon to the current sequence:

   ```ts
   server.del(TodoSchema, 'stale');
   server.gc();
   await t.sync(); // re-bootstraps; 'stale' is gone locally, nothing thrown
   ```

   A client that last synced before the call receives a `reset` on its next pull and transparently re-bootstraps.

7. Hold pushes in flight:

   Call `server.holdPushes()` to pause pushes at the server and test intermediate user interface states:

   ```ts
   const hold = t.server.holdPushes();
   const cycle = t.sync(); // don't await yet
   // … assert spinners, status(), optimistic reads …
   hold.release();
   await cycle;
   ```

   `t.server.pushes` records each `PushRequest` as it arrives, whether held or released.

8. Test custom mutators:

   Register the server implementation of a custom mutator to verify client and server behavior and ensure divergence resolves on rebase:

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

   Throw `Rejection` from a mutator to reject the mutation and discard staged writes.

## Confirming the results

Run your test runner to execute the test suite. Verify that all sync cycles complete and all assertions pass without unhandled rejections or timeouts.

## Related

- [Test kit](../reference/typescript-client.md#test-kit)
