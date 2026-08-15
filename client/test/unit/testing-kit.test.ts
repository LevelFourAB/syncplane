import { describe, expect, it } from 'vitest';

import { CodeConflict, CodeInvalidArgument, CodePermissionDenied } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { createTestClient, createTestServer, Rejection } from '../../src/testing/index.js';

// The app-author test kit, driven the way an app's own test suite would
// drive it. Each spec pins one of the behaviors the kit exists to make
// testable without the Go testserver.

describe('syncplane/testing', () => {
	it('applies writes optimistically and converges on sync', async () => {
		const t = await createTestClient({ entities: [ TodoSchema ] });
		try {
			await t.db.todo.mutate.put({ id: 't1', title: 'Buy milk' });

			// Optimistic: visible locally at once, not on the server until a cycle.
			expect((await t.db.todo.get('t1'))?.title).toBe('Buy milk');
			expect(t.server.get(TodoSchema, 't1')).toBeUndefined();

			await t.sync();
			expect(t.server.get(TodoSchema, 't1')?.title).toBe('Buy milk');
			expect(t.rejections).toHaveLength(0);
		} finally {
			await t.close();
		}
	});

	it('rolls back a rejected offline write when the client comes back online', async () => {
		const t = await createTestClient({ entities: [ TodoSchema ] });
		try {
			await t.net.offline();
			await t.db.todo.mutate.put({ id: 't1', title: 'Not allowed' });
			expect((await t.db.todo.get('t1'))?.title).toBe('Not allowed');

			t.server.reject({ entity: TodoSchema, key: 't1' }, CodePermissionDenied);
			await t.net.online();

			expect(t.rejections).toHaveLength(1);
			expect(t.rejections[0]!.errorCode).toBe(CodePermissionDenied);
			expect(t.rejections[0]!.targets).toEqual([ { entity: 'todo', key: 't1' } ]);
			// The rollback the user sees: the optimistic row is gone.
			expect(await t.db.todo.get('t1')).toBeUndefined();
			expect(t.server.get(TodoSchema, 't1')).toBeUndefined();
		} finally {
			await t.close();
		}
	});

	it('converges two clients by per-field last-writer-wins', async () => {
		const server = createTestServer({ entities: [ TodoSchema ] });
		const a = await createTestClient({ server });
		const b = await createTestClient({ server });
		try {
			await a.db.todo.mutate.put({ id: 't1', title: 'Original' });
			await a.sync();
			await b.sync();

			// Concurrent edits to different fields of the same row.
			await a.db.todo.mutate.patch('t1', { title: 'Renamed' });
			await b.db.todo.mutate.patch('t1', { completed: true });

			await a.sync();
			await b.sync(); // pushes b's patch, pulls a's
			await a.sync(); // pulls b's

			for (const t of [ a, b ]) {
				const row = await t.db.todo.get('t1');
				expect(row?.title).toBe('Renamed');
				expect(row?.completed).toBe(true);
			}
		} finally {
			await a.close();
			await b.close();
		}
	});

	it('delivers seeded and server-side writes on the next sync', async () => {
		const server = createTestServer({ entities: [ TodoSchema ] });
		server.put(TodoSchema, { id: 'seeded', title: 'Already there' });

		const t = await createTestClient({ server });
		try {
			await t.sync();
			expect((await t.db.todo.get('seeded'))?.title).toBe('Already there');

			server.put(TodoSchema, { id: 'later', title: 'From elsewhere' });
			await t.sync();
			expect((await t.db.todo.get('later'))?.title).toBe('From elsewhere');
		} finally {
			await t.close();
		}
	});

	it('re-bootstraps transparently when GC has collected past the cursor', async () => {
		const server = createTestServer({ entities: [ TodoSchema ] });
		server.put(TodoSchema, { id: 'keep', title: 'Keep' });
		server.put(TodoSchema, { id: 'drop', title: 'Drop' });

		const events: string[] = [];
		const t = await createTestClient({ server, onSyncEvent: (e) => events.push(e.type) });
		try {
			await t.sync();
			expect(await t.db.todo.get('drop')).toBeDefined();
			const bootstrapsBefore = events.filter((e) => e === 'bootstrap').length;

			// The delete's tombstone is collected before the client ever sees
			// it — a pull can no longer converge, only a fresh snapshot can.
			server.del(TodoSchema, 'drop');
			expect(server.gc()).toBe(1);

			await t.sync();
			expect(await t.db.todo.get('drop')).toBeUndefined();
			expect((await t.db.todo.get('keep'))?.title).toBe('Keep');
			expect(events.filter((e) => e === 'bootstrap').length).toBe(bootstrapsBefore + 1);
		} finally {
			await t.close();
		}
	});

	it('rejects a patch of a row that does not exist as a conflict', async () => {
		const t = await createTestClient({ entities: [ TodoSchema ] });
		try {
			await t.db.todo.mutate.patch('missing', { title: 'Ghost' });
			await t.sync();
			expect(t.rejections[0]!.errorCode).toBe(CodeConflict);
			expect(await t.db.todo.get('missing')).toBeUndefined();
		} finally {
			await t.close();
		}
	});

	it('rejects a compare-and-swap against a moved version and rolls back', async () => {
		const server = createTestServer({ entities: [ TodoSchema ] });
		server.put(TodoSchema, { id: 't1', title: 'Server state' });

		const t = await createTestClient({ server });
		try {
			await t.sync();
			// expectedVersion 0n = "only if the row does not exist" — it does.
			await t.db.todo.mutate.put({ id: 't1', title: 'Clobbered' }, { expectedVersion: 0n });
			expect((await t.db.todo.get('t1'))?.title).toBe('Clobbered');

			await t.sync();
			expect(t.rejections[0]!.errorCode).toBe(CodeConflict);
			expect((await t.db.todo.get('t1'))?.title).toBe('Server state');
		} finally {
			await t.close();
		}
	});

	it('holds pushes at the server door until released', async () => {
		const t = await createTestClient({ entities: [ TodoSchema ] });
		try {
			const hold = t.server.holdPushes();
			await t.db.todo.mutate.put({ id: 't1', title: 'Held' });

			const cycle = t.sync();
			await new Promise((resolve) => setTimeout(resolve, 0));
			expect(t.server.pushes).toHaveLength(1);
			expect(t.server.get(TodoSchema, 't1')).toBeUndefined();

			hold.release();
			await cycle;
			expect(t.server.get(TodoSchema, 't1')?.title).toBe('Held');
		} finally {
			await t.close();
		}
	});

	it('runs server-side custom mutators, atomically and rejectably', async () => {
		const server = createTestServer({ entities: [ TodoSchema ] });
		server.mutator('todo.clearCompleted', {
			apply(tx) {
				const done = tx.scan(TodoSchema).filter((row) => row.completed);
				if (done.length === 0) throw new Rejection(CodeConflict, 'nothing to clear');
				for (const row of done) tx.del(TodoSchema, row.id);
			},
		});
		const t = await createTestClient({ server });
		try {
			await t.db.todo.mutate.put({ id: 'a', title: 'A', completed: true });
			await t.db.todo.mutate.put({ id: 'b', title: 'B' });
			await t.sync();

			await t.client.mutate.custom('todo.clearCompleted');
			await t.sync();
			expect(t.server.get(TodoSchema, 'a')).toBeUndefined();
			expect(t.server.get(TodoSchema, 'b')).toBeDefined();
			// No client mirror was registered, so the delete lands on rebase.
			expect(await t.db.todo.get('a')).toBeUndefined();

			// A second run has nothing left to clear and rejects.
			await t.client.mutate.custom('todo.clearCompleted');
			await t.sync();
			expect(t.rejections[0]!.errorCode).toBe(CodeConflict);
		} finally {
			await t.close();
		}
	});

	it('rejects an unregistered custom mutator as invalid_argument', async () => {
		const t = await createTestClient({ entities: [ TodoSchema ] });
		try {
			await t.client.mutate.custom('nope');
			await t.sync();
			expect(t.rejections[0]!.errorCode).toBe(CodeInvalidArgument);
			expect(t.rejections[0]!.errorMessage).toContain('unknown mutator "nope"');
		} finally {
			await t.close();
		}
	});

	it('surfaces a revoked workspace on status()', async () => {
		const t = await createTestClient({ entities: [ TodoSchema ] });
		try {
			await t.sync();
			t.server.revoke('main');
			await t.db.todo.mutate.put({ id: 't1', title: 'Denied' });
			await t.sync().catch(() => undefined);
			expect(t.client.status().workspaces['main']?.access).toBe('revoked');
		} finally {
			await t.close();
		}
	});
});
