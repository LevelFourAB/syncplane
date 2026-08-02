import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { CodeConflict } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

describe('expected-version CAS', () => {
	it('exposes the confirmed base version via getRow and scan', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();

		// A locally-created row has no confirmed base yet.
		await a.mutate.put(TodoSchema, { id: 't1', title: 'v1' });
		expect((await a.getRow(TodoSchema, 't1'))?.version).toBe(0n);

		// After the round-trip it carries the workspace sequence that wrote it.
		await a.syncNow();
		const row = await a.getRow(TodoSchema, 't1');
		expect(row?.value.title).toBe('v1');
		expect(row?.version).toBe(1n);
		const [ scanned ] = await a.scan(TodoSchema);
		expect(scanned?.version).toBe(1n);

		await a.close();
	});

	it('rejects a CAS patch whose base version is stale, then succeeds after re-reading', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const rejections: RejectionInfo[] = [];
		const b = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await b.start();

		// Both clients converge on t1 at version 1.
		await a.mutate.put(TodoSchema, { id: 't1', title: 'original' });
		await a.syncNow();
		await b.syncNow();
		const base = await b.getRow(TodoSchema, 't1');
		expect(base?.version).toBe(1n);

		// A moves the row server-side (version 1 -> 2) behind B's back.
		await a.mutate.patch(TodoSchema, 't1', { title: 'moved by A' });
		await a.syncNow();

		// B's CAS patch still expects version 1: the server rejects it and B rolls back.
		await b.mutate.patch(TodoSchema, 't1', { title: 'edited by B' }, {
			expectedVersion: base!.version,
		});
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('edited by B'); // optimistic until pushed
		await b.syncNow();
		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodeConflict);
		expect(await b.pendingCount()).toBe(0);

		// B pulls A's change, re-reads the fresh version, and retries successfully.
		await b.syncNow();
		const fresh = await b.getRow(TodoSchema, 't1');
		expect(fresh?.version).toBe(2n);
		await b.mutate.patch(TodoSchema, 't1', { completed: true }, { expectedVersion: fresh!.version });
		await b.syncNow();
		expect(rejections).toHaveLength(1);

		await a.syncNow();
		for (const client of [ a, b ]) {
			const todo = await client.get(TodoSchema, 't1');
			expect(todo?.title).toBe('moved by A');
			expect(todo?.completed).toBe(true);
		}

		await a.close();
		await b.close();
	});

	it('applies a put guarded by expectedVersion 0 only while the row is absent', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const rejections: RejectionInfo[] = [];
		const b = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await b.start();

		// A create-if-absent succeeds against an empty key.
		await a.mutate.put(TodoSchema, { id: 't1', title: 'by A' }, { expectedVersion: 0n });
		await a.syncNow();
		await b.syncNow();
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('by A');

		// B's create-if-absent for the same key now loses: the row exists.
		await b.mutate.put(TodoSchema, { id: 't1', title: 'by B' }, { expectedVersion: 0n });
		await b.syncNow();
		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodeConflict);
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('by A');

		await a.close();
		await b.close();
	});

	it('applies an unguarded burst on one key without conflicting with itself', async () => {
		// What a text input does: one PATCH per keystroke, none of them awaited.
		// Per-field LWW is the whole contract here, and no edit in the burst is
		// concurrent with anything but this client's own earlier edits.
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't1', title: '' });
		await a.syncNow();

		const titles = [ 'f', 'fi', 'fix', 'fix ', 'fix i', 'fix it' ];
		await Promise.all(titles.map((title) => a.mutate.patch(TodoSchema, 't1', { title })));
		await a.syncNow();
		await a.syncNow();

		expect(rejections).toEqual([]);
		expect(await a.pendingCount()).toBe(0);
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('fix it');

		await a.close();
	});

	it('folds a guarded burst enqueued in one cycle into a single guarded mutation', async () => {
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't1', title: '' });
		await a.syncNow();

		// Every edit in the burst names the same base: only a pull moves the
		// confirmed version, and none can run between these enqueues. Unfolded,
		// the first to reach the server would bump the row past the guard and
		// every later one would conflict with its own predecessor.
		const base = (await a.getRow(TodoSchema, 't1'))!.version;
		await Promise.all(
			[ 'f', 'fi', 'fix' ].map((title) =>
				a.mutate.patch(TodoSchema, 't1', { title }, { expectedVersion: base })
			),
		);
		await a.syncNow();
		await a.syncNow();

		expect(rejections).toEqual([]);
		expect(await a.pendingCount()).toBe(0);
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('fix');

		// The fold keeps the guard rather than dropping it: a foreign write still
		// defeats the whole burst, as one rejection covering one mutation.
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		await b.mutate.patch(TodoSchema, 't1', { completed: true });
		await b.syncNow();

		const stale = (await a.getRow(TodoSchema, 't1'))!.version;
		await Promise.all(
			[ 'x', 'xy' ].map((title) =>
				a.mutate.patch(TodoSchema, 't1', { title }, { expectedVersion: stale })
			),
		);
		await a.syncNow();
		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodeConflict);
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('fix');

		await a.close();
		await b.close();
	});
});
