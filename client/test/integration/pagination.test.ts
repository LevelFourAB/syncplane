import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// The testserver runs with -chunk-size=8, so bootstraps of >8 rows stream
// multiple chunks (see global-setup.ts).

describe('paginated sync', () => {
	it('pulls a large change set through the pagination loop', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		for (let i = 0; i < 30; i++) {
			await writer.mutate.put(TodoSchema, {
				id: `t${String(i).padStart(2, '0')}`,
				title: `todo ${i}`,
			});
		}
		await writer.syncNow();

		// Bootstrap the reader first so subsequent changes arrive via pull.
		const reader = newClient(ws, { pullLimit: 3 });
		await reader.start();
		await reader.syncNow();
		expect(await reader.scan(TodoSchema)).toHaveLength(30);

		// 10 more changes arrive in pages of 3.
		for (let i = 30; i < 40; i++) {
			await writer.mutate.put(TodoSchema, { id: `t${i}`, title: `todo ${i}` });
		}
		await writer.syncNow();
		await reader.syncNow();
		expect(await reader.scan(TodoSchema)).toHaveLength(40);

		await writer.close();
		await reader.close();
	});

	it('bootstraps multiple chunks while a pending offline mutation survives replay', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		for (let i = 0; i < 20; i++) {
			await writer.mutate.put(TodoSchema, {
				id: `t${String(i).padStart(2, '0')}`,
				title: `server ${i}`,
			});
		}
		await writer.syncNow();

		// The late client edits offline before its first sync; the bootstrap
		// must not clobber the pending local edit.
		const late = newClient(ws);
		await late.mutate.put(TodoSchema, { id: 'local', title: 'offline edit' });
		await late.mutate.patch(TodoSchema, 't00', { title: 'patched offline' });
		await late.start();
		await late.syncNow();

		const rows = await late.scan(TodoSchema);
		expect(rows).toHaveLength(21);
		expect((await late.get(TodoSchema, 'local'))?.title).toBe('offline edit');
		expect((await late.get(TodoSchema, 't00'))?.title).toBe('patched offline');

		// After the push round-trips, the server confirms both.
		await late.syncNow();
		expect(await late.pendingCount()).toBe(0);
		expect((await late.get(TodoSchema, 't00'))?.title).toBe('patched offline');

		await writer.close();
		await late.close();
	});

	it('propagates tombstones across a paginated pull', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		for (let i = 0; i < 10; i++) {
			await writer.mutate.put(TodoSchema, { id: `t${i}`, title: `todo ${i}` });
		}
		await writer.syncNow();

		const reader = newClient(ws, { pullLimit: 2 });
		await reader.start();
		await reader.syncNow();
		expect(await reader.scan(TodoSchema)).toHaveLength(10);

		for (const id of [ 't1', 't3', 't5', 't7' ]) {
			await writer.mutate.del(TodoSchema, id);
		}
		await writer.mutate.put(TodoSchema, { id: 't10', title: 'new' });
		await writer.syncNow();

		await reader.syncNow();
		const keys = (await reader.scan(TodoSchema)).map((r) => r.key);
		expect(keys).toEqual([ 't0', 't10', 't2', 't4', 't6', 't8', 't9' ]);

		await writer.close();
		await reader.close();
	});
});
