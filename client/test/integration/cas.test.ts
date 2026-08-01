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
});
